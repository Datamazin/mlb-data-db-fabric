"""
Create SUM DAX measures for all numeric columns in the gold-layer fact tables
of the Fabric semantic model associated with the "MLB Report" in the MLB workspace.

The script:
  1. Resolves the workspace and semantic model IDs (report name → dataset/model).
  2. Downloads the current TMDL definition via the Fabric REST API.
  3. Injects a ``SUM`` measure for every numeric fact column into each table's
     TMDL file, skipping columns whose measure already exists.
  4. Pushes the updated definition back to Fabric.

Usage::

    uv run python scripts/create_measures.py
    uv run python scripts/create_measures.py --workspace "MLB" --report "MLB Report"
    uv run python scripts/create_measures.py --dry-run

Environment variables::

    FABRIC_WORKSPACE_NAME   Workspace name (default: "mlb")
    FABRIC_REPORT_NAME      Report / semantic-model display name (default: "MLB Report")
    ONELAKE_WORKSPACE_ID    Workspace GUID — skips the workspace name-lookup when set
    AZURE_CLIENT_ID         Service-principal credentials (optional)
    AZURE_CLIENT_SECRET     Service-principal credentials (optional)
    AZURE_TENANT_ID         Service-principal credentials (optional)
"""

from __future__ import annotations

import argparse
import base64
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connections import (
    _default_azure_credential,
    _powerbi_token,
    _resolve_dataset_id,
    _resolve_workspace_id,
)

_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_FABRIC_BASE = "https://api.fabric.microsoft.com/v1"


def _fabric_token() -> str:
    """Return an access token for the Fabric REST API."""
    return _default_azure_credential().get_token(_FABRIC_SCOPE).token


def _resolve_semantic_model_id(
    fabric_token: str,
    powerbi_token: str,
    workspace_id: str,
    report_name: str,
) -> str:
    """Resolve semantic model id by name.

    In Fabric/Power BI, semantic model ids align with dataset ids in the same workspace.
    """
    _ = fabric_token

    # First, allow direct semantic-model (dataset) name lookups.
    try:
        return _resolve_dataset_id(powerbi_token, workspace_id, report_name)
    except ValueError:
        pass

    # If the provided name is a report name, resolve its bound dataset id.
    resp = httpx.get(
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports",
        headers={"Authorization": f"Bearer {powerbi_token}"},
        timeout=30,
    )
    resp.raise_for_status()
    reports = resp.json().get("value", [])

    wanted = report_name.strip().lower()
    for report in reports:
        name = str(report.get("name", "")).strip().lower()
        if name == wanted and report.get("datasetId"):
            return str(report["datasetId"])

    available_reports = ", ".join(
        sorted(str(r.get("name", "")) for r in reports if r.get("name"))
    )
    raise ValueError(
        f"No semantic model or report named '{report_name}' found in workspace '{workspace_id}'. "
        f"Available reports: [{available_reports}]"
    )


def _poll_lro(location_url: str, token: str, timeout_seconds: int = 180) -> dict[str, Any]:
    """Poll a Fabric long-running operation URL until completion."""
    deadline = time.time() + timeout_seconds
    headers = {"Authorization": f"Bearer {token}"}
    while time.time() < deadline:
        resp = httpx.get(location_url, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
        status = str(payload.get("status", "")).lower()
        if status in {"succeeded", "success", "completed"}:
            return payload
        if status in {"failed", "error", "cancelled"}:
            raise RuntimeError(f"Fabric operation failed: {payload}")
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for Fabric operation: {location_url}")


def _decode_definition_parts(parts: list[dict[str, Any]]) -> list[dict[str, str]]:
    decoded: list[dict[str, str]] = []
    for part in parts:
        path = str(part.get("path", "")).strip()
        payload = str(part.get("payload", ""))
        payload_type = str(part.get("payloadType", "InlineBase64"))
        if not path:
            continue
        if "base64" in payload_type.lower():
            content = base64.b64decode(payload).decode("utf-8")
        else:
            content = payload
        decoded.append({"path": path, "content": content})
    return decoded


def _get_semantic_model_definition(token: str, workspace_id: str, model_id: str) -> list[dict[str, str]]:
    """Fetch the semantic model definition and return text parts as {path, content}."""
    url = f"{_FABRIC_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = httpx.post(url, headers=headers, json={}, timeout=60)

    if resp.status_code == 202:
        location = resp.headers.get("Location") or resp.headers.get("location")
        if not location:
            raise RuntimeError("Fabric getDefinition returned 202 without a Location header")
        _poll_lro(location, token)
        result_resp = httpx.get(f"{location}/result", headers=headers, timeout=60)
        result_resp.raise_for_status()
        payload = result_resp.json() if result_resp.content else {}
    else:
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}

    definition = payload.get("definition", payload)
    parts = definition.get("parts", []) if isinstance(definition, dict) else []
    if not isinstance(parts, list):
        raise RuntimeError(f"Unexpected getDefinition payload shape: {payload}")
    return _decode_definition_parts(parts)


def _update_semantic_model_definition(
    token: str,
    workspace_id: str,
    model_id: str,
    files: list[dict[str, str]],
) -> None:
    """Push an updated semantic model definition to Fabric."""
    url = f"{_FABRIC_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/updateDefinition"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    parts = [
        {
            "path": f["path"],
            "payload": base64.b64encode(f["content"].encode("utf-8")).decode("ascii"),
            "payloadType": "InlineBase64",
        }
        for f in files
    ]

    resp = httpx.post(
        url,
        headers=headers,
        json={"definition": {"parts": parts}},
        timeout=120,
    )

    if resp.status_code == 202:
        location = resp.headers.get("Location") or resp.headers.get("location")
        if not location:
            raise RuntimeError("Fabric updateDefinition returned 202 without a Location header")
        _poll_lro(location, token)
        return

    resp.raise_for_status()

# ---------------------------------------------------------------------------
# Fact tables and the numeric columns that get a SUM measure.
# Keys must match the table display names in the Fabric semantic model
# (which correspond to gold-layer table names).
# ---------------------------------------------------------------------------

#: Baseball abbreviations that should stay ALL-CAPS in measure display names.
_ABBREVS = frozenset(
    {
        "ab",
        "ba",
        "babip",
        "bb",
        "era",
        "fip",
        "hr",
        "ip",
        "k9",
        "bb9",
        "hr9",
        "obp",
        "ops",
        "pa",
        "rbi",
        "slg",
        "whip",
    }
)

#: Integer and decimal numeric columns per gold fact table.
FACT_SUM_COLUMNS: dict[str, list[str]] = {
    "fact_game": [
        "home_score",
        "away_score",
        "innings",
        "attendance",
        "game_duration_min",
        "series_game_num",
    ],
    "leaderboards": [
        "games",
        "pa",
        "ab",
        "hits",
        "home_runs",
        "rbi",
        "runs",
        "walks",
        "strikeouts",
        "stolen_bases",
        "avg",
        "obp",
        "slg",
        "ops",
        "babip",
    ],
    "player_season_summary": [
        "games",
        "pa",
        "ab",
        "hits",
        "doubles",
        "triples",
        "home_runs",
        "rbi",
        "runs",
        "walks",
        "strikeouts",
        "stolen_bases",
        "caught_stealing",
        "avg",
        "obp",
        "slg",
        "ops",
        "babip",
    ],
    "head_to_head": [
        "wins",
        "losses",
        "games_played",
    ],
    "standings_snap": [
        "wins",
        "losses",
        "win_pct",
        "games_back",
        "last_10_wins",
        "last_10_losses",
        "home_wins",
        "home_losses",
        "away_wins",
        "away_losses",
        "run_diff",
    ],
    "league_averages": [
        "league_avg",
        "league_obp",
        "league_slg",
        "league_ops",
        "league_era",
    ],
}

DISPLAY_FOLDER = "SUM Measures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col_to_display(col: str) -> str:
    """Convert a snake_case column name to a human-friendly measure display name.

    Known baseball abbreviations are uppercased; all other words are title-cased.

    Examples::

        home_score       → "Total Home Score"
        pa               → "Total PA"
        obp              → "Total OBP"
        game_duration_min → "Total Game Duration Min"
    """
    words = col.split("_")
    formatted = [w.upper() if w.lower() in _ABBREVS else w.capitalize() for w in words]
    return "Total " + " ".join(formatted)


def _measure_name(col: str) -> str:
    return _col_to_display(col)


def _measure_block(table: str, col: str) -> str:
    """Return the TMDL text for a single SUM measure (tab-indented)."""
    name = _measure_name(col)
    return (
        f"\n\tmeasure '{name}' = SUM('{table}'[{col}])\n"
        f"\t\tdisplayFolder: {DISPLAY_FOLDER}\n"
    )


def _inject_measures(tmdl: str, table: str, columns: list[str], dry_run: bool) -> tuple[str, int]:
    """Append missing SUM measures to a table's TMDL content.

    Returns the (possibly updated) TMDL string and the count of measures added.
    """
    added = 0
    new_blocks: list[str] = []
    for col in columns:
        name = _measure_name(col)
        # Skip if a measure with this name already exists in the file.
        if f"measure '{name}'" in tmdl:
            continue
        new_blocks.append(_measure_block(table, col))
        added += 1

    if new_blocks and not dry_run:
        tmdl = tmdl.rstrip("\n") + "\n" + "".join(new_blocks)

    return tmdl, added


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def create_measures(workspace_name: str, report_name: str, dry_run: bool) -> None:
    print(f"Workspace : {workspace_name}")
    print(f"Report    : {report_name}")
    if dry_run:
        print("Mode      : DRY RUN — no changes will be written\n")
    else:
        print("Mode      : LIVE\n")

    pbi_token = _powerbi_token()
    fab_token = _fabric_token()

    workspace_id = _resolve_workspace_id(pbi_token, workspace_name)
    print(f"Workspace ID : {workspace_id}")

    model_id = _resolve_semantic_model_id(fab_token, pbi_token, workspace_id, report_name)
    print(f"Model ID     : {model_id}\n")

    print("Fetching TMDL definition…")
    files = _get_semantic_model_definition(fab_token, workspace_id, model_id)
    print(f"  {len(files)} part(s) retrieved\n")

    # Build a path → index map for quick lookup.
    path_index: dict[str, int] = {f["path"]: i for i, f in enumerate(files)}

    total_added = 0

    for table, columns in FACT_SUM_COLUMNS.items():
        # Fabric TMDL table files live under tables/<tableName>.tmdl
        candidate_paths = [
            f"definition/tables/{table}.tmdl",
            f"tables/{table}.tmdl",
            f"model/{table}.tmdl",
            f"{table}.tmdl",
        ]
        file_idx: int | None = None
        for cp in candidate_paths:
            if cp in path_index:
                file_idx = path_index[cp]
                break

        if file_idx is None:
            print(f"  [SKIP] '{table}' — TMDL file not found in definition")
            continue

        original = files[file_idx]["content"]
        updated, added = _inject_measures(original, table, columns, dry_run)
        files[file_idx]["content"] = updated
        total_added += added

        status = "DRY RUN" if dry_run else "updated"
        print(f"  {table}: {added} measure(s) added ({status})")

    print(f"\nTotal SUM measures added: {total_added}")

    if total_added == 0:
        print("Nothing to update — all measures already exist.")
        return

    if dry_run:
        print("Dry-run complete — no changes written to Fabric.")
        return

    print("\nPushing updated definition to Fabric…")
    _update_semantic_model_definition(fab_token, workspace_id, model_id, files)
    print("Done ✓")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create SUM DAX measures for all gold-layer fact tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--workspace",
        default=os.environ.get("FABRIC_WORKSPACE_NAME", "mlb"),
        metavar="NAME",
        help="Fabric workspace name (default: %(default)s)",
    )
    parser.add_argument(
        "--report",
        default=os.environ.get("FABRIC_REPORT_NAME", "MLB Report"),
        metavar="NAME",
        help="Report or semantic-model display name (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print which measures would be added without writing to Fabric",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    create_measures(
        workspace_name=args.workspace,
        report_name=args.report,
        dry_run=args.dry_run,
    )
