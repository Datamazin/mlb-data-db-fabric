import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from connections import get_warehouse_conn, get_onelake_fs, get_bronze_root
from extractor.client import MLBClient
from extractor.models.player import PersonResponse
from extractor.writer import BronzeWriter, player_to_record
from transformer.transform import Transformer
from aggregator.aggregate import Aggregator


async def main() -> None:
    conn = get_warehouse_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT gb.player_id
        FROM silver.game_batting gb
        JOIN silver.games g ON gb.game_pk = g.game_pk
        WHERE g.season_year = 2025
          AND NOT EXISTS (
              SELECT 1 FROM silver.players p WHERE p.player_id = gb.player_id
          )
    """)
    missing_ids = [row[0] for row in cursor.fetchall()]
    print(f"Missing player IDs: {missing_ids}")

    if not missing_ids:
        print("Nothing to fix.")
        conn.close()
        return

    fs = get_onelake_fs()
    bronze_root = get_bronze_root()
    writer = BronzeWriter(fs, bronze_root)
    records = []

    async with MLBClient() as client:
        for pid in missing_ids:
            try:
                raw = await client.get(f"/v1/people/{pid}")
                resp = PersonResponse.model_validate(raw)
                if resp.person:
                    raw_person = raw.get("people", [{}])[0]
                    records.append(player_to_record(resp.person, raw_person, f"/v1/people/{pid}"))
                    print(f"Fetched {pid}: {records[-1].get('full_name')}")
            except Exception as e:
                print(f"ERROR fetching {pid}: {e}")

    if records:
        writer.write_players(records, season_year=2025)

        transformer = Transformer(conn=conn, fs=fs, bronze_root=bronze_root)
        transformer.run(scripts=["006_players.sql"], force=True)

        aggregator = Aggregator(conn)
        aggregator.run(
            scripts=["001_dim_player.sql", "006_leaderboards.sql", "007_player_season_summary.sql"],
            force=True,
        )
        print(f"Done — {len(records)} players backfilled.")
    else:
        print("No records fetched from API.")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
