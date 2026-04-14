"""
fetch_daily_stats.py
Pulls probable starters from the MLB Stats API and
Statcast splits from Baseball Savant via pybaseball.
Run manually or via GitHub Actions (see .github/workflows/daily_stats.yml).
"""

import json
import logging
import os
from datetime import date

import pandas as pd
import requests
from pybaseball import cache, statcast_pitcher

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
cache.enable()

SEASON_START = "2025-03-20"   # update each new season
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


# ── MLB Stats API ─────────────────────────────────────────────────────────────
def get_probable_starters(game_date=None):
    """Return a list of probable-starter dicts for every game on game_date."""
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {
        "sportId": 1,
        "date": game_date,
        "hydrate": "probablePitcher,team",
    }

    log.info("Fetching schedule for %s ...", game_date)
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    dates = data.get("dates", [])
    if not dates:
        log.warning("No games found for %s", game_date)
        return []

    starters = []
    for game in dates[0].get("games", []):
        home = game["teams"]["home"]
        away = game["teams"]["away"]
        starters.append(
            {
                "game_id":           game["gamePk"],
                "game_time":         game.get("gameDate", ""),
                "home_team":         home["team"]["abbreviation"],
                "away_team":         away["team"]["abbreviation"],
                "home_pitcher_id":   home.get("probablePitcher", {}).get("id"),
                "home_pitcher_name": home.get("probablePitcher", {}).get("fullName", "TBD"),
                "away_pitcher_id":   away.get("probablePitcher", {}).get("id"),
                "away_pitcher_name": away.get("probablePitcher", {}).get("fullName", "TBD"),
            }
        )

    log.info("Found %d games", len(starters))
    return starters


# ── Statcast splits ───────────────────────────────────────────────────────────
def compute_statcast_splits(df):
    """Derive key Statcast metrics from a raw pitch-level DataFrame."""
    if df.empty:
        return {}

    bbe    = df[df["type"] == "X"]
    swings = df[df["description"].isin(
        ["swinging_strike", "foul", "hit_into_play", "swinging_strike_blocked"]
    )]
    whiffs = df[df["description"].isin(
        ["swinging_strike", "swinging_strike_blocked"]
    )]
    called_strikes = df[df["description"] == "called_strike"]
    csw = pd.concat([whiffs, called_strikes])

    xwoba_col = df["estimated_woba_using_speedangle"].dropna()
    ev_col    = bbe["launch_speed"].dropna()
    velo_col  = df["release_speed"].dropna()

    splits = {
        "pitches":       int(len(df)),
        "xwoba_allowed": round(xwoba_col.mean(), 3) if not xwoba_col.empty else None,
        "barrel_rate":   round(bbe["barrel"].mean(), 3) if not bbe.empty else None,
        "avg_exit_velo": round(ev_col.mean(), 1) if not ev_col.empty else None,
        "whiff_pct":     round(len(whiffs) / len(swings), 3) if len(swings) > 0 else None,
        "csw_pct":       round(len(csw) / len(df), 3) if len(df) > 0 else None,
        "avg_velo":      round(velo_col.mean(), 1) if not velo_col.empty else None,
    }

    # Top-3 pitch-type mix
    if "pitch_type" in df.columns:
        mix = (
            df["pitch_type"]
            .value_counts(normalize=True)
            .head(3)
            .mul(100)
            .round(1)
            .to_dict()
        )
        splits["pitch_mix"] = mix

    return splits


def get_pitcher_statcast(mlbam_id, season_start=SEASON_START, season_end=None):
    """Fetch and summarise Statcast data for a single pitcher."""
    if season_end is None:
        season_end = date.today().strftime("%Y-%m-%d")

    log.info("  Pulling Statcast for pitcher %s ...", mlbam_id)
    try:
        df = statcast_pitcher(season_start, season_end, player_id=mlbam_id)
        return compute_statcast_splits(df)
    except Exception as exc:
        log.error("  Failed for pitcher %s: %s", mlbam_id, exc)
        return {}


# ── Daily report builder ──────────────────────────────────────────────────────
def build_daily_report(game_date=None):
    """Combine schedule + Statcast splits into a single DataFrame."""
    starters = get_probable_starters(game_date)
    if not starters:
        return pd.DataFrame()

    rows = []
    for game in starters:
        row = {
            "game_id":   game["game_id"],
            "game_time": game["game_time"],
            "matchup":   f"{game['away_team']} @ {game['home_team']}",
        }

        for side in ("home", "away"):
            pid  = game[f"{side}_pitcher_id"]
            name = game[f"{side}_pitcher_name"]
            row[f"{side}_pitcher"]    = name
            row[f"{side}_pitcher_id"] = pid

            if pid:
                splits = get_pitcher_statcast(pid)
                for k, v in splits.items():
                    if k == "pitch_mix":
                        row[f"{side}_pitch_mix"] = json.dumps(v)
                    else:
                        row[f"{side}_{k}"] = v
            else:
                log.info("  No probable pitcher for %s (%s)", side, game["matchup"])

        rows.append(row)

    return pd.DataFrame(rows)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    today = date.today().strftime("%Y-%m-%d")
    log.info("=== MLB Daily Starter Stats — %s ===", today)

    df = build_daily_report()

    if df.empty:
        log.warning("No data to save.")
    else:
        # Latest (overwritten daily — what Streamlit reads)
        out = os.path.join(DATA_DIR, "daily_starters.csv")
        df.to_csv(out, index=False)
        log.info("Saved %d rows -> %s", len(df), out)

        # Dated snapshot for history
        snap = os.path.join(DATA_DIR, f"starters_{today}.csv")
        df.to_csv(snap, index=False)
        log.info("Snapshot -> %s", snap)

    log.info("Done.")
