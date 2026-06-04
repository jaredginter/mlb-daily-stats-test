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
import sys
from datetime import date, datetime, timedelta

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
# Pitcher game log
# ─────────────────────────────────────────────────────────────────────────────

def get_pitcher_game_log(pitcher_id, season=None):
    """
    Fetch a pitcher's game-by-game log for the current season
    from the MLB Stats API. Returns a DataFrame with one row per start.
    """
    if season is None:
        season = date.today().year

    url    = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
    params = {
        "stats":  "gameLog",
        "group":  "pitching",
        "season": season,
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        splits = r.json().get("stats", [{}])[0].get("splits", [])
    except Exception as exc:
        log.error("  Game log fetch failed for pitcher %s: %s", pitcher_id, exc)
        return pd.DataFrame()

    if not splits:
        return pd.DataFrame()

    rows = []
    for s in splits:
        stat = s.get("stat", {})
        game = s.get("game", {})
        team = s.get("team", {})
        opp  = s.get("opponent", {})
        rows.append({
            "date":       s.get("date", ""),
            "opponent":   opp.get("abbreviation", ""),
            "home_away":  "vs" if s.get("isHome") else "@",
            "result":     f"W {stat.get('wins',0)}-{stat.get('losses',0)}" if stat.get("wins") else
                          f"L {stat.get('wins',0)}-{stat.get('losses',0)}" if stat.get("losses") else "ND",
            "ip":         stat.get("inningsPitched", ""),
            "h":          stat.get("hits", ""),
            "r":          stat.get("runs", ""),
            "er":         stat.get("earnedRuns", ""),
            "hr":         stat.get("homeRuns", ""),
            "bb":         stat.get("baseOnBalls", ""),
            "k":          stat.get("strikeOuts", ""),
            "pitches":    stat.get("numberOfPitches", ""),
        })

    df = pd.DataFrame(rows)
    # Most recent starts first
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    df["date"] = df["date"].dt.strftime("%-m/%-d")
    return df

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
        "abs":           int(ab_df["events"].isin(ab_events).sum()) if "events" in ab_df.columns else int(vs["at_bat_number"].nunique()),
        "seasons":       seasons_seen,
        "batting_avg":   batting_avg,
        "home_runs":     home_runs,
        "xwoba":         round(xwoba_vals.mean(), 3) if not xwoba_vals.empty else None,
        "hard_hit_pct":  round((bbe["launch_speed"] >= 95).sum() / len(bbe), 3)
                         if (has_launch_spd and not bbe.empty) else None,
        "whiff_pct":     round(len(whiffs) / len(swings), 3) if len(swings) > 0 else None,
        "avg_exit_velo": round(ev_vals.mean(), 1) if not ev_vals.empty else None,
    }


# FIP constant by season (league average — close enough for display purposes)
# Updated through 2025; defaults to 3.10 for unknown seasons
FIP_CONSTANTS = {
    2015: 3.134, 2016: 3.147, 2017: 3.158, 2018: 3.161,
    2019: 3.214, 2020: 3.191, 2021: 3.170, 2022: 3.125,
    2023: 3.148, 2024: 3.131, 2025: 3.10,
}


def compute_pitcher_fip_vs_team(pitcher_df, opposing_batter_ids):
    """
    Compute a pitcher's FIP against a specific set of batters (the opposing team)
    using only plate appearance ending events from the Statcast data.

    FIP = ((13 x HR) + (3 x BB) - (2 x K)) / IP + FIP_constant
    """
    if pitcher_df.empty or not opposing_batter_ids:
        return None

    # Filter to only this team's batters
    vs = pitcher_df[pitcher_df["batter"].isin(opposing_batter_ids)].copy()
    if vs.empty:
        return None

    # Only look at plate-appearance ending pitches
    pa_endings = vs[vs["events"].notna() & (vs["events"] != "")]

    if pa_endings.empty:
        return None

    hr = (pa_endings["events"] == "home_run").sum()
    bb = pa_endings["events"].isin(["walk", "intent_walk"]).sum()
    k  = pa_endings["events"].isin(["strikeout", "strikeout_double_play"]).sum()

    # Estimate innings pitched: each out = 1/3 inning
    # Outs = AB events that don't result in hit, walk, HBP, or error
    out_events = {
        "field_out", "force_out", "grounded_into_double_play",
        "double_play", "triple_play", "strikeout",
        "strikeout_double_play", "fielders_choice_out",
        "other_out", "sac_fly", "sac_bunt",
    }
    outs = pa_endings["events"].isin(out_events).sum()
    ip   = outs / 3.0

    if ip < 1.0:
        return None  # not enough sample to be meaningful

    # Use the most common season in the data for the FIP constant
    if "game_date" in vs.columns:
        years = pd.to_datetime(vs["game_date"], errors="coerce").dt.year.dropna()
        season = int(years.mode().iloc[0]) if not years.empty else 2025
    else:
        season = 2025

    fip_c = FIP_CONSTANTS.get(season, 3.10)
    fip   = ((13 * hr) + (3 * bb) - (2 * k)) / ip + fip_c

    return round(fip, 2)


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
        return pd.DataFrame(), None

    # Guard: ensure we got a proper DataFrame back, not a tuple or None
    if not isinstance(pitcher_df, pd.DataFrame):
        log.warning("  Unexpected return type for %s: %s", pitcher_name, type(pitcher_df))
        return pd.DataFrame(), None

    if pitcher_df.empty:
        log.warning("  No Statcast data found for %s", pitcher_name)
        return pd.DataFrame(), None

    log.info("  Got %d pitches — slicing by opposing hitters ...", len(pitcher_df))

    # Compute FIP vs this entire opposing team using the full pitcher_df
    opposing_ids = [h["mlbam_id"] for h in hitters]
    fip_vs_team  = compute_pitcher_fip_vs_team(pitcher_df, opposing_ids)
    if fip_vs_team:
        log.info("  FIP vs this team: %.2f", fip_vs_team)
    else:
        log.info("  FIP vs this team: insufficient data")

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
        return pd.DataFrame(), fip_vs_team

    return pd.DataFrame(rows).sort_values("xwoba", ascending=False, na_position="last"), fip_vs_team


# ─────────────────────────────────────────────────────────────────────────────
# Daily report builder
# ─────────────────────────────────────────────────────────────────────────────

def build_daily_report(game_date=None, splits_dir=None, logs_dir=None):
    """
    For each game:
      - Fetch home pitcher career data → slice vs away lineup
      - Fetch away pitcher career data → slice vs home lineup
    Saves per-game CSVs to splits_dir (defaults to SPLITS_DIR).
    Returns a summary DataFrame (one row per game).
    """
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")
    if splits_dir is None:
        splits_dir = SPLITS_DIR
    if logs_dir is None:
        logs_dir = os.path.join(DATA_DIR, "gamelogs")
    os.makedirs(splits_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

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

        # ── Game logs for both pitchers ─────────────────────────────────────
        for side in ("home", "away"):
            pid = game[f"{side}_pitcher_id"]
            if pid:
                gl = get_pitcher_game_log(pid)
                if not gl.empty:
                    gl_path = os.path.join(logs_dir, f"{pid}_gamelog.csv")
                    os.makedirs(os.path.dirname(gl_path), exist_ok=True)
                    gl.to_csv(gl_path, index=False)

        # ── Home pitcher vs away lineup ──────────────────────────────────────
        if game["home_pitcher_id"]:
            away_hitters          = get_active_hitters(game["away_team_id"])
            away_splits, away_fip = get_lineup_splits_vs_pitcher(
                away_hitters, game["home_pitcher_id"], game["home_pitcher_name"]
            )
            row["home_pitcher_fip_vs_opp"] = away_fip
            if not away_splits.empty:
                path = os.path.join(splits_dir, f"{game_id}_away_vs_home_pitcher.csv")
                away_splits.to_csv(path, index=False)
                row["away_hitters_with_history"] = len(away_splits)
                row["away_lineup_avg_xwoba"]     = round(away_splits["xwoba"].dropna().mean(), 3)
                # Total career ABs across all hitters vs this pitcher
                row["away_total_abs"]            = int(away_splits["abs"].sum()) if "abs" in away_splits.columns else 0
            else:
                row["away_hitters_with_history"] = 0
                row["away_lineup_avg_xwoba"]     = None
                row["away_total_abs"]            = 0
        else:
            row["away_hitters_with_history"]   = None
            row["away_lineup_avg_xwoba"]       = None
            row["away_total_abs"]              = None
            row["home_pitcher_fip_vs_opp"]     = None

        # ── Away pitcher vs home lineup ──────────────────────────────────────
        if game["away_pitcher_id"]:
            home_hitters          = get_active_hitters(game["home_team_id"])
            home_splits, home_fip = get_lineup_splits_vs_pitcher(
                home_hitters, game["away_pitcher_id"], game["away_pitcher_name"]
            )
            row["away_pitcher_fip_vs_opp"] = home_fip
            if not home_splits.empty:
                path = os.path.join(splits_dir, f"{game_id}_home_vs_away_pitcher.csv")
                home_splits.to_csv(path, index=False)
                row["home_hitters_with_history"] = len(home_splits)
                row["home_lineup_avg_xwoba"]     = round(home_splits["xwoba"].dropna().mean(), 3)
                row["home_total_abs"]            = int(home_splits["abs"].sum()) if "abs" in home_splits.columns else 0
            else:
                row["home_hitters_with_history"] = 0
                row["home_lineup_avg_xwoba"]     = None
                row["home_total_abs"]            = 0
        else:
            row["home_hitters_with_history"]   = None
            row["home_lineup_avg_xwoba"]       = None
            row["home_total_abs"]              = None
            row["away_pitcher_fip_vs_opp"]     = None

        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

# Tomorrow's data goes in its own subfolder so it never collides with today
TOMORROW_DIR        = os.path.join(DATA_DIR, "tomorrow")
TOMORROW_SPLITS_DIR = os.path.join(TOMORROW_DIR, "hitter_splits")
TOMORROW_LOGS_DIR   = os.path.join(TOMORROW_DIR, "gamelogs")


def clear_stale_data(target_dir=None, splits_subdir=None, logs_subdir=None):
    """
    Delete all hitter split and game log CSVs before each run so stale
    data from previous days never bleeds into today's dashboard.
    """
    if target_dir is None:
        target_dir   = DATA_DIR
        splits_subdir = SPLITS_DIR
        logs_subdir   = os.path.join(DATA_DIR, "gamelogs")

    cleared = 0
    for folder in [splits_subdir, logs_subdir]:
        if folder and os.path.exists(folder):
            for f in os.listdir(folder):
                if f.endswith(".csv"):
                    os.remove(os.path.join(folder, f))
                    cleared += 1
    log.info("Cleared %d stale CSV files", cleared)


def save_report(df, game_date, target_dir, splits_subdir, logs_subdir, label=""):
    """Write summary CSV + snapshot + timestamp for a given date's report."""
    if df.empty:
        log.warning("No data to save%s.", f" ({label})" if label else "")
        return

    os.makedirs(target_dir, exist_ok=True)

    out = os.path.join(target_dir, "daily_starters.csv")
    df.to_csv(out, index=False)
    log.info("%sSummary -> %s (%d games)", f"[{label}] " if label else "", out, len(df))

    snap = os.path.join(target_dir, f"starters_{game_date}.csv")
    df.to_csv(snap, index=False)
    log.info("Snapshot -> %s", snap)

    ts_path = os.path.join(target_dir, "last_updated.txt")
    with open(ts_path, "w") as f:
        f.write(datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))
    log.info("Timestamp written -> %s", ts_path)


def build_tomorrow_report(tomorrow_str):
    """
    Fetch tomorrow's starters and save splits/gamelogs to data/tomorrow/.
    Passes directory params directly — no module-level variable swapping needed.
    """
    os.makedirs(TOMORROW_SPLITS_DIR, exist_ok=True)
    os.makedirs(TOMORROW_LOGS_DIR, exist_ok=True)
    return build_daily_report(
        tomorrow_str,
        splits_dir=TOMORROW_SPLITS_DIR,
        logs_dir=TOMORROW_LOGS_DIR,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Accept optional --tomorrow-only flag for targeted runs
    tomorrow_only = "--tomorrow-only" in sys.argv

    today_str    = date.today().strftime("%Y-%m-%d")
    tomorrow_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    # UTC hour tells us which scheduled run this is
    # 03:00 UTC = 9 PM CST → the evening run that should also fetch tomorrow
    utc_hour = datetime.utcnow().hour
    is_evening_run = (utc_hour >= 2 and utc_hour <= 4)  # 9 PM CST window

    log.info("=== MLB Hitter Splits vs Starters — %s ===", today_str)
    log.info("UTC hour: %d | Evening run: %s | Tomorrow-only: %s",
             utc_hour, is_evening_run, tomorrow_only)

    # ── Today's data ────────────────────────────────────────────────────────
    if not tomorrow_only:
        clear_stale_data()
        df_today = build_daily_report(today_str)
        save_report(df_today, today_str, DATA_DIR, SPLITS_DIR,
                    os.path.join(DATA_DIR, "gamelogs"), label="today")

    # ── Tomorrow's data (evening run only, or if explicitly requested) ──────
    if is_evening_run or tomorrow_only:
        log.info("=== Fetching tomorrow's starters — %s ===", tomorrow_str)
        clear_stale_data(
            target_dir   = TOMORROW_DIR,
            splits_subdir= TOMORROW_SPLITS_DIR,
            logs_subdir  = TOMORROW_LOGS_DIR,
        )
        df_tomorrow = build_tomorrow_report(tomorrow_str)
        save_report(df_tomorrow, tomorrow_str, TOMORROW_DIR,
                    TOMORROW_SPLITS_DIR, TOMORROW_LOGS_DIR, label="tomorrow")
    else:
        log.info("Skipping tomorrow fetch (not evening run). "
                 "Re-run with --tomorrow-only to force.")

    log.info("Done.")
