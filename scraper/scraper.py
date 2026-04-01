#!/usr/bin/env python3
"""
scraper.py — baseball-live Live Game Pitch Scraper

Fetches all pitches for a single MLB game from Baseball Savant /gf endpoint.
Writes to data/data.csv (overwrites each run).

Usage:
    GAME_PK=777483 python scraper/scraper.py
"""

import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

# ── Config ─────────────────────────────────────────────────────────────────────
SCHEDULE_URL = "https://baseballsavant.mlb.com/schedule?date={year}-{month}-{day}"
GAME_URL     = "https://baseballsavant.mlb.com/gf?game_pk={game_pk}"

ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = DATA_DIR / "data.csv"

# ── Column Whitelist ────────────────────────────────────────────────────────────
WHITELIST = [
    "game_pk", "game_date", "home_team", "away_team", "type", "play_id",
    "inning", "ab_number", "cap_index", "outs",
    "batter", "stand", "batter_name",
    "pitcher", "p_throws", "pitcher_name",
    "team_batting", "team_fielding", "team_batting_id", "team_fielding_id",
    "result", "des", "events", "contextMetrics",
    "strikes", "balls", "pre_strikes", "pre_balls",
    "call", "call_name", "pitch_call", "is_strike_swinging",
    "result_code",
    "pitch_type", "pitch_name", "description",
    "start_speed", "end_speed",
    "sz_top", "sz_bot",
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ", "breakZ",
    "px", "pz", "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth",
    "savantIsInZone", "isInZone", "isSword", "is_bip_out", "is_abs_challenge",
    "plate_x", "plate_z",
    "pitch_number", "player_total_pitches", "player_total_pitches_pitch_types",
    "pitcher_pa_number", "pitcher_time_thru_order", "game_total_pitches",
    "batSpeed", "hit_distance", "xba", "is_barrel", "hc_x_ft", "hc_y_ft",
    "hit_speed", "hit_angle", "launch_speed", "launch_angle",
    "runnerOn1B", "runnerOn2B", "runnerOn3B",
    "is_last_pitch",
    "double_header", "game_number",
]

NUMERIC_COLS = [
    "game_pk", "inning", "ab_number", "cap_index", "outs",
    "batter", "pitcher", "team_batting_id", "team_fielding_id",
    "strikes", "balls", "pre_strikes", "pre_balls",
    "start_speed", "end_speed", "sz_top", "sz_bot",
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ", "breakZ",
    "px", "pz", "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth", "plate_x", "plate_z",
    "pitch_number", "player_total_pitches", "player_total_pitches_pitch_types",
    "pitcher_pa_number", "pitcher_time_thru_order", "game_total_pitches",
    "batSpeed", "hit_distance", "hit_speed", "hit_angle",
    "hc_x_ft", "hc_y_ft", "launch_speed", "launch_angle", "game_number",
]
ROUND_2 = [
    "start_speed", "end_speed", "sz_top", "sz_bot", "extension", "plateTime",
    "spin_rate", "breakX", "inducedBreakZ", "breakZ",
    "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth", "batSpeed", "hit_distance", "hit_speed", "hit_angle",
    "hc_x_ft", "hc_y_ft", "launch_speed", "launch_angle", "xba",
]
ROUND_4 = ["plate_x", "plate_z", "px", "pz"]
BOOL_COLS = ["is_strike_swinging", "savantIsInZone", "isInZone", "isSword", "is_abs_challenge"]


# ── Schedule lookup ─────────────────────────────────────────────────────────────

def get_game_meta(game_pk: int) -> dict:
    """Look up home/away team info from today's schedule. Returns safe defaults if not found."""
    today = date.today()
    url = SCHEDULE_URL.format(year=today.year, month=today.month, day=today.day)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        dates = data.get("schedule", {}).get("dates", [])
        if dates:
            for g in dates[0].get("games", []):
                if g.get("gamePk") == game_pk:
                    teams = g.get("teams", {})
                    return {
                        "game_pk":       game_pk,
                        "home_team":     teams.get("home", {}).get("team", {}).get("abbreviation", ""),
                        "away_team":     teams.get("away", {}).get("team", {}).get("abbreviation", ""),
                        "double_header": g.get("doubleHeader", "N"),
                        "game_number":   int(g.get("gameNumber", 1)),
                        "status":        g.get("status", {}).get("detailedState", ""),
                    }
    except Exception as exc:
        print(f"WARNING: schedule lookup failed: {exc}", file=sys.stderr)

    # Not found in today's schedule — return minimal meta; team fields come from pitch rows
    return {
        "game_pk":       game_pk,
        "home_team":     "",
        "away_team":     "",
        "double_header": "N",
        "game_number":   1,
        "status":        "unknown",
    }


# ── Pitch fetch ─────────────────────────────────────────────────────────────────

def fetch_game_pitches(game_meta: dict) -> list[dict]:
    game_pk = game_meta["game_pk"]
    url = GAME_URL.format(game_pk=game_pk)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        print(f"ERROR: /gf fetch failed for game_pk={game_pk}: {exc}", file=sys.stderr)
        return []

    game_date = data.get("game_date", "")
    home_team = game_meta["home_team"]
    away_team = game_meta["away_team"]

    rows = []
    for side in ("home_pitchers", "away_pitchers"):
        pitcher_dict = data.get(side, {})
        if not isinstance(pitcher_dict, dict):
            continue
        for pitcher_id, pitch_list in pitcher_dict.items():
            if not isinstance(pitch_list, list):
                continue
            for item in pitch_list:
                if not isinstance(item, dict) or "play_id" not in item:
                    continue
                row = dict(item)
                row["game_date"]     = game_date
                row["home_team"]     = home_team or row.get("home_team", "")
                row["away_team"]     = away_team or row.get("away_team", "")
                row["double_header"] = game_meta["double_header"]
                row["game_number"]   = game_meta["game_number"]
                row["game_pk"]       = int(row.get("game_pk", game_pk))
                rows.append(row)

    return rows


# ── Clean ───────────────────────────────────────────────────────────────────────

def _context_metrics_to_str(val):
    if val is None:
        return None
    if isinstance(val, dict):
        return None if not val else json.dumps(val)
    s = str(val).strip()
    return None if s in ("", "{}", "None") else s


def compute_is_last_pitch(df: pd.DataFrame) -> pd.Series:
    gtp = pd.to_numeric(df["game_total_pitches"], errors="coerce")
    max_gtp = gtp.groupby([df["game_pk"], df["ab_number"]]).transform("max")
    is_last = gtp == max_gtp
    if "type" in df.columns:
        is_last = is_last.where(~df["type"].eq("no_pitch"), other=pd.NA)
    return is_last


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "game_total_pitches" in df.columns and "ab_number" in df.columns:
        df["is_last_pitch"] = compute_is_last_pitch(df)

    if "contextMetrics" in df.columns:
        df["contextMetrics"] = df["contextMetrics"].apply(_context_metrics_to_str)

    cols_available = [c for c in WHITELIST if c in df.columns]
    df = df[cols_available].copy()

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "game_pk"     in df.columns: df["game_pk"]     = df["game_pk"].astype("Int64")
    if "game_number" in df.columns: df["game_number"] = df["game_number"].astype("Int64")
    if "game_date"   in df.columns: df["game_date"]   = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    if "xba"         in df.columns: df["xba"]         = pd.to_numeric(df["xba"], errors="coerce")
    if "is_barrel"   in df.columns: df["is_barrel"]   = pd.to_numeric(df["is_barrel"], errors="coerce").astype("Int64")
    if "is_bip_out"  in df.columns: df["is_bip_out"]  = df["is_bip_out"].map({"Y": True, "N": False, True: True, False: False})

    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].map({True: True, False: False})

    if "is_last_pitch" in df.columns:
        df["is_last_pitch"] = df["is_last_pitch"].map({True: True, False: False, pd.NA: None})

    for col in ("runnerOn1B", "runnerOn2B", "runnerOn3B"):
        if col in df.columns:
            df[col] = df[col].map({True: True, False: False, None: None})

    for col in ROUND_2:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    for col in ROUND_4:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    return df


def sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [c for c in ["game_pk", "game_total_pitches"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=True, na_position="last")
    return df.reset_index(drop=True)


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    game_pk_str = os.environ.get("GAME_PK", "").strip()
    if not game_pk_str:
        print("ERROR: GAME_PK environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    try:
        game_pk = int(game_pk_str)
    except ValueError:
        print(f"ERROR: GAME_PK must be an integer, got: {game_pk_str!r}", file=sys.stderr)
        sys.exit(1)

    print(f"[scraper] game_pk={game_pk}")

    # Look up game metadata from today's schedule
    print("[scraper] Fetching schedule metadata...")
    game_meta = get_game_meta(game_pk)
    matchup = f"{game_meta['away_team'] or '???'}@{game_meta['home_team'] or '???'}"
    print(f"[scraper] Game: {matchup}  status={game_meta['status']}")

    # Fetch pitch data
    print(f"[scraper] Fetching pitches from Baseball Savant...")
    rows = fetch_game_pitches(game_meta)
    print(f"[scraper] Raw rows fetched: {len(rows)}")

    if not rows:
        print("[scraper] No pitch data returned. Writing empty CSV.")
        pd.DataFrame().to_csv(OUT_PATH, index=False)
        print(f"[scraper] Wrote empty {OUT_PATH}")
        return

    df = pd.DataFrame(rows)
    df = clean_dataframe(df)
    df = sort_dataframe(df)

    df.to_csv(OUT_PATH, index=False, encoding="utf-8")

    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[scraper] ✓ {matchup}  rows={len(df)}  ts={ts}")
    print(f"[scraper] Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
