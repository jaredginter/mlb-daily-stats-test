"""
fetch_daily_stats.py
For each day's probable starters, fetches the pitcher's full career
Statcast data once, then slices it per opposing hitter.

This is much faster than the previous approach of fetching each hitter
individually — 1 API call per pitcher instead of ~13 per pitcher.

Output: data/daily_starters.csv  (one row per game)
        data/hitter_splits/      (one CSV per game: hitter-vs-pitcher rows)
"""

import logging
import os
from datetime import date

import pandas as pd
import requests
from pybaseball import cache, statcast_pitcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

cache.enable()

# Full Statcast era — pulls all career history vs each pitcher
STATCAST_ERA_START = "2015-03-01"
DATA_DIR           = "data"
SPLITS_DIR         = os.path.join(DATA_DIR, "hitter_splits")
os.makedirs(SPLITS_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# MLB Stats API helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_probable_starters(game_date=None):
    """Return schedule rows with probable pitcher IDs + team IDs for today."""
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    url    = "https://statsapi.mlb.com/api/v1/schedule"
    params = {"sportId": 1, "date": game_date, "hydrate": "probablePitcher,team"}

    log.info("Fetching schedule for %s ...", game_date)
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    dates = r.json().get("dates", [])
    if not dates:
        log.warning("No games found for %s", game_date)
        return []

    starters = []
    for game in dates[0].get("games", []):
        home = game["teams"]["home"]
        away = game["teams"]["away"]
        starters.append({
            "game_id":           game["gamePk"],
            "game_time":         game.get("gameDate", ""),
            "home_team":         home["team"]["abbreviation"],
            "away_team":         away["team"]["abbreviation"],
            "home_team_id":      home["team"]["id"],
            "away_team_id":      away["team"]["id"],
            "home_pitcher_id":   home.get("probablePitcher", {}).get("id"),
            "home_pitcher_name": home.get("probablePitcher", {}).get("fullName", "TBD"),
            "away_pitcher_id":   away.get("probablePitcher", {}).get("id"),
            "away_pitcher_name": away.get("probablePitcher", {}).get("fullName", "TBD"),
        })

    log.info("Found %d games", len(starters))
    return starters


def get_active_hitters(team_id):
    """Return {name, mlbam_id} for every active non-pitcher on a team."""
    url    = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
    params = {"rosterType": "active"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        roster = r.json().get("roster", [])
    except Exception as exc:
        log.error("  Roster fetch failed for team %s: %s", team_id, exc)
        return []

    return [
        {"name": p["person"]["fullName"], "mlbam_id": p["person"]["id"]}
        for p in roster
        if p.get("position", {}).get("abbreviation", "") != "P"
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Statcast splits — pitcher-first approach
# ─────────────────────────────────────────────────────────────────────────────

def compute_hitter_splits(pitcher_df, batter_mlbam_id):
    """
    Slice a pitcher's career Statcast DataFrame down to one batter
    and compute summary metrics.
    """
    vs = pitcher_df[pitcher_df["batter"] == batter_mlbam_id].copy()
    if vs.empty:
        return None

    bbe    = vs[vs["type"] == "X"]
    swings = vs[vs["description"].isin(
        ["swinging_strike", "foul", "hit_into_play", "swinging_strike_blocked"]
    )]
    whiffs = vs[vs["description"].isin(
        ["swinging_strike", "swinging_strike_blocked"]
    )]

    # Year range of the matchup history
    seasons_seen = ""
    if "game_date" in vs.columns:
        years = pd.to_datetime(vs["game_date"], errors="coerce").dt.year.dropna().astype(int)
        if not years.empty:
            yr_min, yr_max = int(years.min()), int(years.max())
            seasons_seen = str(yr_min) if yr_min == yr_max else f"{yr_min}–{yr_max}"

    # Guard against missing columns — pybaseball omits these when data is sparse
    has_barrel     = "barrel" in bbe.columns
    has_launch_spd = "launch_speed" in bbe.columns
    has_xwoba      = "estimated_woba_using_speedangle" in vs.columns

    xwoba_vals = vs["estimated_woba_using_speedangle"].dropna() if has_xwoba else pd.Series([], dtype=float)
    ev_vals    = bbe["launch_speed"].dropna() if has_launch_spd else pd.Series([], dtype=float)

    # Batting average: hits / at-bats (exclude walks, HBP, sac flies)
    # Statcast events that count as hits
    hit_events = {"single", "double", "triple", "home_run"}
    ab_events  = {
        "single", "double", "triple", "home_run",
        "strikeout", "strikeout_double_play",
        "field_out", "force_out", "grounded_into_double_play",
        "fielders_choice", "fielders_choice_out",
        "double_play", "triple_play",
    }
    # One row per plate appearance (use last pitch of each AB)
    ab_df = vs.sort_values("pitch_number").groupby("at_bat_number").last()
    hits  = ab_df["events"].isin(hit_events).sum() if "events" in ab_df.columns else 0
    abs_  = ab_df["events"].isin(ab_events).sum()  if "events" in ab_df.columns else 0
    batting_avg = round(hits / abs_, 3) if abs_ > 0 else None

    # Home runs: count PA events == "home_run"
    home_runs = int((ab_df["events"] == "home_run").sum()) if "events" in ab_df.columns else 0

    return {
        "pa":            int(vs["at_bat_number"].nunique()),
        "seasons":       seasons_seen,
        "batting_avg":   batting_avg,
        "home_runs":     home_runs,
        "xwoba":         round(xwoba_vals.mean(), 3) if not xwoba_vals.empty else None,
        "hard_hit_pct":  round((bbe["launch_speed"] >= 95).sum() / len(bbe), 3)
                         if (has_launch_spd and not bbe.empty) else None,
        "whiff_pct":     round(len(whiffs) / len(swings), 3) if len(swings) > 0 else None,
        "avg_exit_velo": round(ev_vals.mean(), 1) if not ev_vals.empty else None,
    }


def get_lineup_splits_vs_pitcher(hitters, pitcher_mlbam_id, pitcher_name):
    """
    Fetch the pitcher's full career Statcast data ONCE, then slice per hitter.
    Returns a DataFrame sorted by xwOBA descending.
    """
    today = date.today().strftime("%Y-%m-%d")

    log.info("  Fetching career Statcast for %s (id=%s) ...", pitcher_name, pitcher_mlbam_id)
    try:
        pitcher_df = statcast_pitcher(STATCAST_ERA_START, today, player_id=pitcher_mlbam_id)
    except Exception as exc:
        log.error("  Failed to fetch pitcher data for %s: %s", pitcher_name, exc)
        return pd.DataFrame()

    if pitcher_df.empty:
        log.warning("  No Statcast data found for %s", pitcher_name)
        return pd.DataFrame()

    log.info("  Got %d pitches — slicing by opposing hitters ...", len(pitcher_df))

    rows = []
    for hitter in hitters:
        splits = compute_hitter_splits(pitcher_df, hitter["mlbam_id"])
        if splits:
            rows.append({
                "batter_name": hitter["name"],
                "batter_id":   hitter["mlbam_id"],
                **splits,
            })
        else:
            log.info("    No history: %s vs %s", hitter["name"], pitcher_name)

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values("xwoba", ascending=False, na_position="last")


# ─────────────────────────────────────────────────────────────────────────────
# Daily report builder
# ─────────────────────────────────────────────────────────────────────────────

def build_daily_report(game_date=None):
    """
    For each game:
      - Fetch home pitcher career data → slice vs away lineup
      - Fetch away pitcher career data → slice vs home lineup
    Saves per-game CSVs to data/hitter_splits/
    Returns a summary DataFrame (one row per game).
    """
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    starters = get_probable_starters(game_date)
    if not starters:
        return pd.DataFrame()

    summary_rows = []

    for game in starters:
        matchup = f"{game['away_team']} @ {game['home_team']}"
        game_id = game["game_id"]
        log.info("Processing: %s", matchup)

        row = {
            "game_id":           game_id,
            "game_time":         game["game_time"],
            "matchup":           matchup,
            "home_team":         game["home_team"],
            "away_team":         game["away_team"],
            "home_pitcher_name": game["home_pitcher_name"],
            "away_pitcher_name": game["away_pitcher_name"],
            "home_pitcher_id":   game["home_pitcher_id"],
            "away_pitcher_id":   game["away_pitcher_id"],
        }

        # ── Home pitcher vs away lineup ──────────────────────────────────────
        if game["home_pitcher_id"]:
            away_hitters = get_active_hitters(game["away_team_id"])
            away_splits  = get_lineup_splits_vs_pitcher(
                away_hitters, game["home_pitcher_id"], game["home_pitcher_name"]
            )
            if not away_splits.empty:
                path = os.path.join(SPLITS_DIR, f"{game_id}_away_vs_home_pitcher.csv")
                away_splits.to_csv(path, index=False)
                row["away_hitters_with_history"] = len(away_splits)
                row["away_lineup_avg_xwoba"]     = round(away_splits["xwoba"].dropna().mean(), 3)
            else:
                row["away_hitters_with_history"] = 0
                row["away_lineup_avg_xwoba"]     = None
        else:
            row["away_hitters_with_history"] = None
            row["away_lineup_avg_xwoba"]     = None

        # ── Away pitcher vs home lineup ──────────────────────────────────────
        if game["away_pitcher_id"]:
            home_hitters = get_active_hitters(game["home_team_id"])
            home_splits  = get_lineup_splits_vs_pitcher(
                home_hitters, game["away_pitcher_id"], game["away_pitcher_name"]
            )
            if not home_splits.empty:
                path = os.path.join(SPLITS_DIR, f"{game_id}_home_vs_away_pitcher.csv")
                home_splits.to_csv(path, index=False)
                row["home_hitters_with_history"] = len(home_splits)
                row["home_lineup_avg_xwoba"]     = round(home_splits["xwoba"].dropna().mean(), 3)
            else:
                row["home_hitters_with_history"] = 0
                row["home_lineup_avg_xwoba"]     = None
        else:
            row["home_hitters_with_history"] = None
            row["home_lineup_avg_xwoba"]     = None

        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    today = date.today().strftime("%Y-%m-%d")
    log.info("=== MLB Hitter Splits vs Starters — %s ===", today)

    df = build_daily_report(today)

    if df.empty:
        log.warning("No data to save.")
    else:
        out = os.path.join(DATA_DIR, "daily_starters.csv")
        df.to_csv(out, index=False)
        log.info("Summary -> %s (%d games)", out, len(df))

        snap = os.path.join(DATA_DIR, f"starters_{today}.csv")
        df.to_csv(snap, index=False)
        log.info("Snapshot -> %s", snap)

    log.info("Done.")
