"""
MLB Analytics — Streamlit frontend.
Entry point / Home page. Run with: make streamlit
"""

import sys
from pathlib import Path

import pandas as pd
import pyodbc
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

load_dotenv()

from src.connections import get_warehouse_conn

st.set_page_config(
    page_title="MLB Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)


def get_conn() -> pyodbc.Connection | None:
    try:
        return get_warehouse_conn()
    except Exception:
        return None


def query_df(conn: pyodbc.Connection, sql: str, params=None) -> pd.DataFrame:
    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    return pd.DataFrame.from_records(cursor.fetchall(), columns=cols)


def main() -> None:
    st.title("MLB Analytics")

    conn = get_conn()
    if conn is None:
        st.error(
            "Could not connect to Fabric Warehouse. "
            "Check FABRIC_CONNECTION_STRING and run `make migrate` to initialise the schema."
        )
        return

    try:
        seasons = conn.execute(
            "SELECT COUNT(DISTINCT season_year) FROM gold.fact_game"
        ).fetchone()[0]
        total_games = conn.execute(
            "SELECT COUNT(*) FROM gold.fact_game WHERE status = 'Final'"
        ).fetchone()[0]
        total_teams = conn.execute(
            "SELECT COUNT(DISTINCT team_id) FROM gold.dim_team WHERE active = 1"
        ).fetchone()[0]
        total_players = conn.execute(
            "SELECT COUNT(*) FROM gold.dim_player WHERE active = 1"
        ).fetchone()[0]

        recent = query_df(conn, """
            SELECT
                game_date,
                away_team_abbrev  AS away,
                away_score,
                home_score,
                home_team_abbrev  AS home,
                venue_name,
                game_type,
                innings
            FROM gold.fact_game
            WHERE status = 'Final'
            ORDER BY game_date DESC, game_pk DESC
            OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY
        """)

        by_season = query_df(conn, """
            SELECT
                season_year                                                        AS season,
                SUM(CASE WHEN status = 'Final' THEN 1 ELSE 0 END)                AS final,
                SUM(CASE WHEN game_type = 'R' THEN 1 ELSE 0 END)                 AS regular,
                SUM(CASE WHEN game_type IN ('F','D','L','W') THEN 1 ELSE 0 END)  AS postseason
            FROM gold.fact_game
            GROUP BY season_year
            ORDER BY season_year
        """)
    finally:
        conn.close()

    # ── Summary metrics ───────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Seasons", seasons)
    c2.metric("Games (Final)", f"{total_games:,}")
    c3.metric("Active Teams", total_teams)
    c4.metric("Active Players", f"{total_players:,}")

    st.divider()

    # ── Recent games ──────────────────────────────────────────────────────────
    st.subheader("Recent Results")

    if recent.empty:
        st.info("No completed games found yet.")
    else:
        st.dataframe(
            recent,
            width="stretch",
            hide_index=True,
            column_config={
                "game_date": st.column_config.DateColumn("Date"),
                "away": "Away",
                "away_score": "R",
                "home_score": "R",
                "home": "Home",
                "venue_name": "Venue",
                "game_type": "Type",
                "innings": "Inn",
            },
        )

    # ── Season breakdown ──────────────────────────────────────────────────────
    st.subheader("Games by Season")

    if not by_season.empty:
        st.dataframe(by_season, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
