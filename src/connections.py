"""
Fabric Warehouse and OneLake connection helpers.

Environment variables
---------------------
FABRIC_CONNECTION_STRING  Full pyodbc connection string (takes precedence).
                          If absent, the parts below are used.
FABRIC_SERVER             e.g. <workspace_id>.datawarehouse.fabric.microsoft.com
FABRIC_DATABASE           Warehouse name
FABRIC_AUTH               Token (default, uses DefaultAzureCredential — supports
                          'az login', VS Code, Managed Identity automatically)
                          | ActiveDirectoryServicePrincipal (CI/CD with explicit creds)
AZURE_CLIENT_ID           Required for ActiveDirectoryServicePrincipal
AZURE_CLIENT_SECRET       Required for ActiveDirectoryServicePrincipal
AZURE_TENANT_ID           Required for ActiveDirectoryServicePrincipal

ONELAKE_WORKSPACE_ID      Fabric workspace GUID (kept for reference)
ONELAKE_WORKSPACE_NAME    Fabric workspace name used as the OneLake filesystem
ONELAKE_LAKEHOUSE_NAME    Lakehouse name (without .Lakehouse suffix)
"""

from __future__ import annotations

import fnmatch
import io
import os
import struct

import pyodbc
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.storage.filedatalake import DataLakeServiceClient

# OAuth resource scope for Azure SQL / Fabric Warehouse
_SQL_SCOPE = "https://database.windows.net/.default"
# pyodbc connection attribute for pre-acquired access token (ODBC Driver 17+)
_SQL_COPT_SS_ACCESS_TOKEN = 1256


def _sql_server_odbc_driver() -> str:
    """Return the best available SQL Server ODBC driver for pyodbc."""
    drivers = set(pyodbc.drivers())
    if "ODBC Driver 18 for SQL Server" in drivers:
        return "ODBC Driver 18 for SQL Server"
    if "ODBC Driver 17 for SQL Server" in drivers:
        return "ODBC Driver 17 for SQL Server"
    raise RuntimeError(
        "No supported SQL Server ODBC driver found. "
        "Install ODBC Driver 18 or 17 for SQL Server."
    )


def _token_attr(token_str: str) -> dict[int, bytes]:
    """Pack an access token string into the pyodbc SQL_COPT_SS_ACCESS_TOKEN format."""
    token_bytes = token_str.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    return {_SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def get_warehouse_conn() -> pyodbc.Connection:
    """Return an open pyodbc connection to the Fabric Warehouse.

    Authentication strategy (FABRIC_AUTH env var):
    - ``Token`` (default): uses ODBC Driver 18 built-in ``ActiveDirectoryDefault``
      which honours ``az login``, VS Code auth, Managed Identity automatically.
      Falls back to ``SQL_COPT_SS_ACCESS_TOKEN`` with ``DefaultAzureCredential``
      when only ODBC Driver 17 is available.
    - ``AzureCli``: ODBC built-in ``ActiveDirectoryAzCli`` (Driver 18) or
      ``AzureCliCredential`` token injection (Driver 17).
    - ``ActiveDirectoryMsi``: ODBC built-in Managed Identity auth (Azure-only).
    - ``ActiveDirectoryServicePrincipal``: SP auth; requires
      ``AZURE_CLIENT_ID``, ``AZURE_CLIENT_SECRET``, ``AZURE_TENANT_ID``.
    """
    conn_str = os.getenv("FABRIC_CONNECTION_STRING")
    if conn_str:
        return pyodbc.connect(conn_str, autocommit=False)

    driver = _sql_server_odbc_driver()
    server = os.environ["FABRIC_SERVER"]
    database = os.environ["FABRIC_DATABASE"]
    auth = os.getenv("FABRIC_AUTH", "Token")

    # ODBC Driver 18 supports built-in Azure AD authentication keywords,
    # which are more reliable than manual token injection for Fabric Warehouse.
    use_driver18_native = driver == "ODBC Driver 18 for SQL Server"

    base = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Encrypt=yes;"
    )

    if auth == "ActiveDirectoryServicePrincipal":
        client_id = os.environ["AZURE_CLIENT_ID"]
        client_secret = os.environ["AZURE_CLIENT_SECRET"]
        tenant_id = os.environ["AZURE_TENANT_ID"]
        if use_driver18_native:
            conn_str = (
                base
                + "Authentication=ActiveDirectoryServicePrincipal;"
                + f"UID={client_id};"
                + f"PWD={client_secret};"
            )
            return pyodbc.connect(conn_str, autocommit=False)
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        token = credential.get_token(_SQL_SCOPE).token
        return pyodbc.connect(base, autocommit=False, attrs_before=_token_attr(token))

    if auth == "ActiveDirectoryMsi":
        if use_driver18_native:
            return pyodbc.connect(base + "Authentication=ActiveDirectoryMsi;", autocommit=False)
        credential: ManagedIdentityCredential | DefaultAzureCredential = ManagedIdentityCredential()
        token = credential.get_token(_SQL_SCOPE).token
        return pyodbc.connect(base, autocommit=False, attrs_before=_token_attr(token))

    if auth == "AzureCli":
        if use_driver18_native:
            return pyodbc.connect(base + "Authentication=ActiveDirectoryAzCli;", autocommit=False)
        credential = AzureCliCredential()  # type: ignore[assignment]
        token = credential.get_token(_SQL_SCOPE).token
        return pyodbc.connect(base, autocommit=False, attrs_before=_token_attr(token))

    sp_configured = bool(os.getenv("AZURE_CLIENT_ID") and os.getenv("AZURE_CLIENT_SECRET"))
    credential = DefaultAzureCredential(  # type: ignore[assignment]
        exclude_environment_credential=not sp_configured,
    )
    token = credential.get_token(_SQL_SCOPE).token
    return pyodbc.connect(base, autocommit=False, attrs_before=_token_attr(token))


def _default_azure_credential() -> DefaultAzureCredential:
    sp_configured = bool(os.getenv("AZURE_CLIENT_ID") and os.getenv("AZURE_CLIENT_SECRET"))
    return DefaultAzureCredential(exclude_environment_credential=not sp_configured)


class _OneLakeWriteBuffer(io.BytesIO):
    def __init__(self, fs: OneLakeFileSystem, path: str) -> None:
        super().__init__()
        self._fs = fs
        self._path = path

    def close(self) -> None:
        if not self.closed:
            self.seek(0)
            self._fs._upload_bytes(self._path, self.getvalue())
        super().close()


class OneLakeFileSystem:
    """Minimal OneLake filesystem backed by the ADLS Gen2 Data Lake client."""

    def __init__(self, workspace_name: str, credential: DefaultAzureCredential) -> None:
        self._workspace_name = workspace_name
        self._service_client = DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=credential,
        )
        self._fs_client = self._service_client.get_file_system_client(workspace_name)

    @staticmethod
    def _normalize(path: str) -> str:
        return path.strip("/")

    @staticmethod
    def _fixed_prefix(pattern: str) -> str:
        parts: list[str] = []
        for part in pattern.split("/"):
            if any(ch in part for ch in "*?[]"):
                break
            parts.append(part)
        return "/".join(parts)

    def glob(self, pattern: str) -> list[str]:
        normalized = self._normalize(pattern)
        prefix = self._fixed_prefix(normalized)
        try:
            paths = list(self._fs_client.get_paths(path=prefix or None, recursive=True))
        except Exception:
            return []

        matches: list[str] = []
        for path in paths:
            if path.is_directory:
                continue
            if fnmatch.fnmatch(path.name, normalized):
                matches.append(path.name)
        return matches

    def open(self, path: str, mode: str = "rb") -> io.BytesIO:
        normalized = self._normalize(path)
        if mode == "rb":
            data = self._fs_client.get_file_client(normalized).download_file().readall()
            return io.BytesIO(data)
        if mode == "wb":
            return _OneLakeWriteBuffer(self, normalized)
        raise ValueError(f"Unsupported open mode: {mode}")

    def exists(self, path: str) -> bool:
        normalized = self._normalize(path)
        try:
            return self._fs_client.get_file_client(normalized).exists()
        except Exception:
            return False

    def _upload_bytes(self, path: str, data: bytes) -> None:
        normalized = self._normalize(path)
        parts = normalized.split("/")
        current = ""
        for part in parts[:-1]:
            current = f"{current}/{part}" if current else part
            try:
                self._fs_client.create_directory(current)
            except Exception:
                pass
        self._fs_client.get_file_client(normalized).upload_data(data, overwrite=True)


def get_onelake_fs() -> OneLakeFileSystem:
    """Return a OneLake filesystem scoped to the configured Fabric workspace."""
    workspace_name = os.environ["ONELAKE_WORKSPACE_NAME"]
    return OneLakeFileSystem(workspace_name=workspace_name, credential=_default_azure_credential())


def get_bronze_root() -> str:
    """Return the lakehouse-relative prefix for bronze parquet files in OneLake."""
    lakehouse_name = os.environ["ONELAKE_LAKEHOUSE_NAME"]
    return f"{lakehouse_name}.Lakehouse/Files/bronze"
