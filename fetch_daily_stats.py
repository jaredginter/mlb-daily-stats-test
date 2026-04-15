"""
fetch_daily_stats.py
For each day's probable starters, fetches the opposing team's
active roster and pulls each hitter's full career Statcast splits
vs. that pitcher (all seasons since the Statcast era began in 2015).

Output: data/daily_starters.csv  (one row per game)
        data/hitter_splits/  (one CSV per game: hitter-vs-pitcher rows)
"""

import logging
import os
from datetime import date

import pandas as pd
import requests
from pybaseball import cache, statcast_batter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

cache.enable()

# Full Statcast era — career history vs pitcher, not just current season
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
    """
    Return a list of {name, mlbam_id} for active position players on a team.
    Uses the MLB Stats API 40-man / active roster endpoint.
    """
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
    params = {"rosterType": "active"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        roster = r.json().get("roster", [])
    except Exception as exc:
        log.error("  Roster fetch failed for team %s: %s", team_id, exc)
        return []

    hitters = []
    for p in roster:
        pos = p.get("position", {}).get("abbreviation", "")
        # Exclude pitchers (P) and two-way players are included
        if pos != "P":
            hitters.append({
                "name":     p["person"]["fullName"],
                "mlbam_id": p["person"]["id"],
            })
    return hitters


# ─────────────────────────────────────────────────────────────────────────────
# Statcast batter splits
# ─────────────────────────────────────────────────────────────────────────────

def compute_batter_splits_vs_pitcher(batter_df, pitcher_mlbam_id):
    """
    Filter a batter's full career Statcast DataFrame to at-bats vs. a specific
    pitcher, then compute summary metrics across all seasons.
    """
    vs = batter_df[batter_df["pitcher"] == pitcher_mlbam_id].copy()
    if vs.empty:
        return None   # no career history vs this pitcher

    bbe    = vs[vs["type"] == "X"]
    swings = vs[vs["description"].isin(
        ["swinging_strike", "foul", "hit_into_play", "swinging_strike_blocked"]
    )]
    whiffs = vs[vs["description"].isin(
        ["swinging_strike", "swinging_strike_blocked"]
    )]

    xwoba_vals = vs["estimated_woba_using_speedangle"].dropna()
    ev_vals    = bbe["launch_speed"].dropna()

    # Determine the range of seasons covered
    seasons_seen = ""
    if "game_date" in vs.columns:
        years = pd.to_datetime(vs["game_date"], errors="coerce").dt.year.dropna().astype(int)
        if not years.empty:
            yr_min, yr_max = int(years.min()), int(years.max())
            seasons_seen = str(yr_min) if yr_min == yr_max else f"{yr_min}–{yr_max}"

    return {
        "pa":            int(vs["at_bat_number"].nunique()),
        "pitches_seen":  int(len(vs)),
        "seasons":       seasons_seen,
        "xwoba":         round(xwoba_vals.mean(), 3) if not xwoba_vals.empty else None,
        "barrel_rate":   round(bbe["barrel"].mean(), 3) if not bbe.empty else None,
        "avg_exit_velo": round(ev_vals.mean(), 1) if not ev_vals.empty else None,
        "whiff_pct":     round(len(whiffs) / len(swings), 3) if len(swings) > 0 else None,
        "hard_hit_pct":  round((bbe["launch_speed"] >= 95).sum() / len(bbe), 3)
                         if not bbe.empty else None,
    }


def get_lineup_splits_vs_pitcher(hitters, pitcher_mlbam_id):
    """
    For a list of hitters, fetch each one's full career Statcast data
    (2015–today) and filter to at-bats vs. pitcher_mlbam_id.
    Returns a DataFrame with one row per hitter who has faced this pitcher.
    """
    today = date.today().strftime("%Y-%m-%d")
    rows  = []

    for hitter in hitters:
        hitter_id = hitter["mlbam_id"]
        log.info("    %s (id=%s) vs pitcher %s", hitter["name"], hitter_id, pitcher_mlbam_id)
        try:
            # Pull full Statcast career for this batter
            df = statcast_batter(STATCAST_ERA_START, today, player_id=hitter_id)
            splits = compute_batter_splits_vs_pitcher(df, pitcher_mlbam_id)
            if splits:
                rows.append({"batter_name": hitter["name"], "batter_id": hitter_id, **splits})
        except Exception as exc:
            log.warning("    Failed for %s: %s", hitter["name"], exc)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values("xwoba", ascending=False, na_position="last")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Daily report builder
# ─────────────────────────────────────────────────────────────────────────────

def build_daily_report(game_date=None):
    """
    For each game:
      - Home pitcher vs. away lineup
      - Away pitcher vs. home lineup
    Saves per-game hitter split CSVs to data/hitter_splits/
    Returns a summary DataFrame (one row per game).
    """
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    starters = get_probable_starters(game_date)
    if not starters:
        return pd.DataFrame()

    summary_rows = []

    for game in starters:
        matchup   = f"{game['away_team']} @ {game['home_team']}"
        game_id   = game["game_id"]
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

        # ── Home pitcher vs away lineup ──
        if game["home_pitcher_id"]:
            log.info("  Away hitters vs %s", game["home_pitcher_name"])
            away_hitters = get_active_hitters(game["away_team_id"])
            away_splits  = get_lineup_splits_vs_pitcher(
                away_hitters, game["home_pitcher_id"]
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

        # ── Away pitcher vs home lineup ──
        if game["away_pitcher_id"]:
            log.info("  Home hitters vs %s", game["away_pitcher_name"])
            home_hitters = get_active_hitters(game["home_team_id"])
            home_splits  = get_lineup_splits_vs_pitcher(
                home_hitters, game["away_pitcher_id"]
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
