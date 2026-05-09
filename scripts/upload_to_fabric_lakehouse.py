"""
Upload pipeline source code to the Fabric Lakehouse so the nightly_pipeline
notebook can import it at runtime.

Uploads the following to Files/mlb_pipeline/ in the target lakehouse:
  - connections.py             (project root)
  - logging_config.py          (src/)
  - src/extractor/             (full package)
  - src/transformer/           (full package)
  - src/aggregator/            (full package)
  - src/run_tracker/           (full package)
  - src/scheduler/jobs.py      (job functions only — no daemon entry point needed)
  - sql/                       (T-SQL scripts referenced by Transformer/Aggregator)

Usage:
    uv run python scripts/upload_to_fabric_lakehouse.py

Requires .env with ONELAKE_WORKSPACE_NAME, ONELAKE_LAKEHOUSE_NAME, and valid
Azure credentials (az login or service principal).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

load_dotenv(PROJECT_ROOT / ".env")

from connections import get_onelake_fs  # noqa: E402

DEST_PREFIX = "mlb_pipeline"

UPLOAD_PATHS: list[tuple[Path, str]] = [
    # (local_path, dest_relative_to_DEST_PREFIX)
    (PROJECT_ROOT / "src" / "connections.py",    "connections.py"),
    (PROJECT_ROOT / "src" / "logging_config.py", "src/logging_config.py"),
]

UPLOAD_DIRS: list[tuple[Path, str]] = [
    (PROJECT_ROOT / "src" / "extractor",    "src/extractor"),
    (PROJECT_ROOT / "src" / "transformer",  "src/transformer"),
    (PROJECT_ROOT / "src" / "aggregator",   "src/aggregator"),
    (PROJECT_ROOT / "src" / "run_tracker",  "src/run_tracker"),
    (PROJECT_ROOT / "src" / "scheduler",    "src/scheduler"),
    (PROJECT_ROOT / "sql",                  "sql"),
]


def _upload_file(fs, local: Path, remote: str) -> None:
    if not local.exists():
        print(f"  SKIP (not found): {local}")
        return
    with local.open("rb") as fh:
        content = fh.read()
    with fs.open(remote, "wb") as fh:
        fh.write(content)
    print(f"  ✓  {local.relative_to(PROJECT_ROOT)}  →  {remote}")


def main() -> None:
    workspace = os.environ["ONELAKE_WORKSPACE_NAME"]
    lakehouse = os.environ["ONELAKE_LAKEHOUSE_NAME"]

    fs = get_onelake_fs()
    bronze_root = f"{workspace}/{lakehouse}.Lakehouse/Files/{DEST_PREFIX}"

    print(f"Uploading to: {bronze_root}\n")

    for local, dest in UPLOAD_PATHS:
        _upload_file(fs, local, f"{bronze_root}/{dest}")

    for local_dir, dest_dir in UPLOAD_DIRS:
        if not local_dir.exists():
            print(f"  SKIP (not found): {local_dir}")
            continue
        for local_file in sorted(local_dir.rglob("*.py")):
            if "__pycache__" in local_file.parts:
                continue
            rel = local_file.relative_to(local_dir)
            _upload_file(fs, local_file, f"{bronze_root}/{dest_dir}/{rel}")

        # Upload SQL files inside sql/ dirs
        for local_file in sorted(local_dir.rglob("*.sql")):
            rel = local_file.relative_to(local_dir)
            _upload_file(fs, local_file, f"{bronze_root}/{dest_dir}/{rel}")

    print("\nUpload complete.")
    print("Next: open nightly_pipeline.ipynb in Fabric, attach mlb_bronze as default lakehouse,")
    print("      set FABRIC_SERVER in the Config cell, then create scheduled triggers.")


if __name__ == "__main__":
    main()
