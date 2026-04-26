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
import os
import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connections import (
    _fabric_token,
    _get_semantic_model_definition,
    _powerbi_token,
    _resolve_semantic_model_id,
    _resolve_workspace_id,
    _update_semantic_model_definition,
)

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
