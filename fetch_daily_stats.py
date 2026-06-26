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
from pybaseball import cache, statcast_pitcher, batting_stats

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
    """
    Return {name, mlbam_id} for every active non-pitcher on a team.

    The MLB Stats API "active" roster type already excludes IL players
    by definition — it only returns players on the active 26-man roster.
    Players on the 10-day, 15-day, or 60-day IL are not included.

    We additionally cross-reference against the "depthChart" roster
    to catch any status flags, but the active roster is the primary
    and reliable source for IL exclusion.
    """
    url    = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
    params = {"rosterType": "active"}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        roster = r.json().get("roster", [])
    except Exception as exc:
        log.error("  Roster fetch failed for team %s: %s", team_id, exc)
        return []

    if not roster:
        log.warning("  Empty roster returned for team %s", team_id)
        return []

    hitters = [
        {"name": p["person"]["fullName"], "mlbam_id": p["person"]["id"]}
        for p in roster
        if p.get("position", {}).get("abbreviation", "") != "P"
    ]

    log.info("  Active hitters for team %s: %d", team_id, len(hitters))
    return hitters




# ─────────────────────────────────────────────────────────────────────────────
# Park factors + FanGraphs team offense
# ─────────────────────────────────────────────────────────────────────────────

PARK_FACTORS = {
    "AZ": 1.05, "ATL": 0.98, "BAL": 1.01, "BOS": 1.03,
    "CHC": 1.02, "CWS": 0.97, "CIN": 1.04, "CLE": 0.97,
    "COL": 1.22, "DET": 0.99, "HOU": 1.03, "KC":  0.98,
    "LAA": 0.97, "LAD": 0.96, "MIA": 0.95, "MIL": 1.00,
    "MIN": 1.01, "NYM": 0.98, "NYY": 1.05, "OAK": 0.97,
    "PHI": 1.02, "PIT": 0.99, "SD":  0.95, "SF":  0.95,
    "SEA": 0.97, "STL": 0.99, "TB":  0.97, "TEX": 1.02,
    "TOR": 1.00, "WSH": 1.00,
}

# FanGraphs team abbreviation → MLB Stats API abbreviation
FG_TO_MLB = {
    "ARI":"AZ",  "ATL":"ATL", "BAL":"BAL", "BOS":"BOS",
    "CHC":"CHC", "CWS":"CWS", "CIN":"CIN", "CLE":"CLE",
    "COL":"COL", "DET":"DET", "HOU":"HOU", "KCR":"KC",
    "LAA":"LAA", "LAD":"LAD", "MIA":"MIA", "MIL":"MIL",
    "MIN":"MIN", "NYM":"NYM", "NYY":"NYY", "OAK":"OAK",
    "PHI":"PHI", "PIT":"PIT", "SDP":"SD",  "SFG":"SF",
    "SEA":"SEA", "STL":"STL", "TBR":"TB",  "TEX":"TEX",
    "TOR":"TOR", "WSN":"WSH",
}


def fetch_team_offense(season=None):
    """
    Pull team-level batting stats from FanGraphs.
    Returns dict keyed by MLB abbreviation with wOBA, wRC+, OBP, OPS+,
    Barrel%, HardHit%, K%, BB%, BABIP.
    """
    if season is None:
        season = date.today().year
    log.info("Fetching FanGraphs team batting stats for %d ...", season)
    try:
        df = batting_stats(season, qual=0, ind=0)
        if "Team" not in df.columns:
            log.warning("  Team column missing from batting_stats output")
            return {}

        def to_rate(val, default):
            """Convert pct that may be expressed as 0-100 or 0-1 to 0-1."""
            try:
                v = float(val or default)
                return round(v / 100 if v > 1 else v, 3)
            except (TypeError, ValueError):
                return default

        result = {}
        for _, r in df.iterrows():
            fg  = str(r.get("Team", "")).strip()
            mlb = FG_TO_MLB.get(fg, fg)
            result[mlb] = {
                "woba":     round(float(r.get("wOBA",   0.312) or 0.312), 3),
                "wrc_plus": round(float(r.get("wRC+",   100)   or 100),   1),
                "obp":      round(float(r.get("OBP",    0.318) or 0.318), 3),
                "ops_plus": round(float(r.get("OPS+",   100)   or 100),   1),
                "barrel":   to_rate(r.get("Barrel%",  0.080), 0.080),
                "hard_hit": to_rate(r.get("HardHit%", 0.380), 0.380),
                "k_pct":    to_rate(r.get("K%",       0.222), 0.222),
                "bb_pct":   to_rate(r.get("BB%",      0.083), 0.083),
                "babip":    round(float(r.get("BABIP",  0.296) or 0.296), 3),
            }
        log.info("  Team offense fetched for %d teams", len(result))
        return result
    except Exception as exc:
        log.error("Failed to fetch team batting stats: %s", exc)
        return {}


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


# League-average HR per fly ball rate (Statcast era average, ~10.5%).
# xFIP substitutes this for actual HRs to remove HR/FB luck from FIP.
LG_HR_FB_RATE = 0.105

# FIP constant by season (league average — close enough for display purposes)
# Updated through 2025; defaults to 3.10 for unknown seasons
FIP_CONSTANTS = {
    2015: 3.134, 2016: 3.147, 2017: 3.158, 2018: 3.161,
    2019: 3.214, 2020: 3.191, 2021: 3.170, 2022: 3.125,
    2023: 3.148, 2024: 3.131, 2025: 3.10,
}


def compute_pitcher_fip_vs_team(pitcher_df, opposing_batter_ids):
    """
    Compute a pitcher's FIP and xFIP against a specific set of batters (the
    opposing team) using only plate appearance ending events from the Statcast data.

    FIP  = ((13 × HR) + (3 × BB) - (2 × K)) / IP + FIP_constant
    xFIP = ((13 × (FB × LG_HR_FB_RATE)) + (3 × BB) - (2 × K)) / IP + FIP_constant

    xFIP replaces actual HRs with expected HRs based on the pitcher's fly-ball
    count and the league-average HR/FB rate, removing HR luck from the equation.

    Returns a dict {"fip": float|None, "xfip": float|None}, or None if there
    is insufficient data.
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

    # Fly balls for xFIP: count from batted ball events only (where bb_type is meaningful).
    # bb_type is populated on contact (type == "X"); counting it across all pitches
    # would include NaN rows and silently return 0 fly balls.
    # If the column is absent entirely, set xfip to None rather than computing with 0 FBs.
    has_bb_type = "bb_type" in vs.columns
    if has_bb_type:
        bbe_rows = vs[vs["type"] == "X"] if "type" in vs.columns else vs
        fb = (bbe_rows["bb_type"] == "fly_ball").sum()
    else:
        fb = None  # signals xFIP cannot be computed

    # Estimate innings pitched: each out = 1/3 inning
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

    # xFIP: swap actual HRs for expected HRs (fly balls × league HR/FB rate).
    # Return None if bb_type was absent so the dashboard shows "—" honestly
    # rather than silently computing with 0 fly balls.
    if fb is not None:
        hr_expected = fb * LG_HR_FB_RATE
        xfip = round(((13 * hr_expected) + (3 * bb) - (2 * k)) / ip + fip_c, 2)
    else:
        xfip = None

    return {"fip": round(fip, 2), "xfip": xfip}


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
        return pd.DataFrame(), None, None

    # Guard: ensure we got a proper DataFrame back, not a tuple or None
    if not isinstance(pitcher_df, pd.DataFrame):
        log.warning("  Unexpected return type for %s: %s", pitcher_name, type(pitcher_df))
        return pd.DataFrame(), None, None

    if pitcher_df.empty:
        log.warning("  No Statcast data found for %s", pitcher_name)
        return pd.DataFrame(), None, None

    log.info("  Got %d pitches — slicing by opposing hitters ...", len(pitcher_df))

    # Compute FIP and xFIP vs this entire opposing team using the full pitcher_df
    opposing_ids   = [h["mlbam_id"] for h in hitters]
    fip_result     = compute_pitcher_fip_vs_team(pitcher_df, opposing_ids)
    fip_vs_team    = fip_result["fip"]  if fip_result else None
    xfip_vs_team   = fip_result["xfip"] if fip_result else None
    if fip_vs_team is not None:
        log.info("  FIP vs this team: %.2f  xFIP: %.2f", fip_vs_team, xfip_vs_team)
    else:
        log.info("  FIP/xFIP vs this team: insufficient data")

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
        return pd.DataFrame(), fip_vs_team, xfip_vs_team

    return pd.DataFrame(rows).sort_values("xwoba", ascending=False, na_position="last"), fip_vs_team, xfip_vs_team


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

    # Fetch team offense once for all games
    team_offense = fetch_team_offense(date.today().year)

    summary_rows = []

    for game in starters:
        matchup = f"{game['away_team']} @ {game['home_team']}"
        game_id = game["game_id"]
        log.info("Processing: %s", matchup)

        away_off    = team_offense.get(game["away_team"], {})
        home_off    = team_offense.get(game["home_team"], {})
        park_factor = PARK_FACTORS.get(game["home_team"], 1.00)

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
            # Park factor
            "park_factor":       park_factor,
            # Away team season offense
            "away_woba":         away_off.get("woba"),
            "away_wrc_plus":     away_off.get("wrc_plus"),
            "away_obp":          away_off.get("obp"),
            "away_ops_plus":     away_off.get("ops_plus"),
            "away_barrel":       away_off.get("barrel"),
            "away_hard_hit":     away_off.get("hard_hit"),
            "away_k_pct":        away_off.get("k_pct"),
            "away_bb_pct":       away_off.get("bb_pct"),
            "away_babip":        away_off.get("babip"),
            # Home team season offense
            "home_woba":         home_off.get("woba"),
            "home_wrc_plus":     home_off.get("wrc_plus"),
            "home_obp":          home_off.get("obp"),
            "home_ops_plus":     home_off.get("ops_plus"),
            "home_barrel":       home_off.get("barrel"),
            "home_hard_hit":     home_off.get("hard_hit"),
            "home_k_pct":        home_off.get("k_pct"),
            "home_bb_pct":       home_off.get("bb_pct"),
            "home_babip":        home_off.get("babip"),
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
            away_hitters                    = get_active_hitters(game["away_team_id"])
            away_splits, away_fip, away_xfip = get_lineup_splits_vs_pitcher(
                away_hitters, game["home_pitcher_id"], game["home_pitcher_name"]
            )
            row["home_pitcher_fip_vs_opp"]  = away_fip
            row["home_pitcher_xfip_vs_opp"] = away_xfip
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
            row["away_hitters_with_history"]    = None
            row["away_lineup_avg_xwoba"]        = None
            row["away_total_abs"]               = None
            row["home_pitcher_fip_vs_opp"]      = None
            row["home_pitcher_xfip_vs_opp"]     = None

        # ── Away pitcher vs home lineup ──────────────────────────────────────
        if game["away_pitcher_id"]:
            home_hitters                    = get_active_hitters(game["home_team_id"])
            home_splits, home_fip, home_xfip = get_lineup_splits_vs_pitcher(
                home_hitters, game["away_pitcher_id"], game["away_pitcher_name"]
            )
            row["away_pitcher_fip_vs_opp"]  = home_fip
            row["away_pitcher_xfip_vs_opp"] = home_xfip
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
            row["home_hitters_with_history"]    = None
            row["home_lineup_avg_xwoba"]        = None
            row["home_total_abs"]               = None
            row["away_pitcher_fip_vs_opp"]      = None
            row["away_pitcher_xfip_vs_opp"]     = None

        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Change detection — fast pre-check before expensive Statcast pulls
# ─────────────────────────────────────────────────────────────────────────────

def starters_changed(game_date: str, summary_csv: str) -> bool:
    """
    Compare live MLB API probable starters against what is saved in
    summary_csv.  Returns True if any pitcher changed, any new game
    appeared, or if no saved data exists yet.

    This is intentionally cheap — one MLB schedule endpoint call, no
    pybaseball — so it can run on every cron tick without burning quota.
    """
    saved_path = os.path.join(*summary_csv.split("/")) if "/" in summary_csv else summary_csv
    if not os.path.exists(saved_path):
        log.info("No saved data at %s — treating as changed.", saved_path)
        return True

    try:
        url    = "https://statsapi.mlb.com/api/v1/schedule"
        params = {"sportId": 1, "date": game_date, "hydrate": "probablePitcher,team"}
        r      = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        dates  = r.json().get("dates", [])
    except Exception as exc:
        log.warning("MLB API change-check failed: %s — assuming changed to be safe.", exc)
        return True

    # No games at all on this date (off day, etc.)
    if not dates:
        log.info("No games returned from MLB API for %s — skipping rebuild.", game_date)
        return False

    # Build {game_id: (home_pitcher_id, away_pitcher_id)} from live data
    live: dict = {}
    for game in dates[0].get("games", []):
        gid    = str(game["gamePk"])
        home_p = game.get("teams", {}).get("home", {}).get("probablePitcher", {}).get("id")
        away_p = game.get("teams", {}).get("away", {}).get("probablePitcher", {}).get("id")
        live[gid] = (home_p, away_p)

    try:
        saved_df = pd.read_csv(saved_path, dtype={"game_id": str})
    except Exception as exc:
        log.warning("Could not read saved CSV (%s): %s — treating as changed.", saved_path, exc)
        return True

    saved_lookup: dict = {}
    for _, row in saved_df.iterrows():
        gid = str(row["game_id"])
        try:
            hp = int(row["home_pitcher_id"]) if pd.notna(row.get("home_pitcher_id")) else None
            ap = int(row["away_pitcher_id"]) if pd.notna(row.get("away_pitcher_id")) else None
        except (ValueError, TypeError):
            hp, ap = None, None
        saved_lookup[gid] = (hp, ap)

    for gid, (live_home, live_away) in live.items():
        if gid not in saved_lookup:
            log.info("New game detected (%s) — rebuild needed.", gid)
            return True
        saved_home, saved_away = saved_lookup[gid]
        if live_home != saved_home or live_away != saved_away:
            log.info(
                "Starter change in game %s: home %s→%s, away %s→%s",
                gid, saved_home, live_home, saved_away, live_away,
            )
            return True

    log.info("No starter changes detected for %s — skipping full rebuild.", game_date)
    return False


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
    """
    Write summary CSV + snapshot + timestamp for a given date's report.
    Never overwrites an existing CSV with an empty result — if the fetch
    returned no games, the previous data is preserved.
    """
    tag = f" ({label})" if label else ""

    if df.empty:
        existing = os.path.join(target_dir, "daily_starters.csv")
        if os.path.exists(existing):
            log.warning("No new data%s — keeping existing CSV to avoid empty dashboard.", tag)
        else:
            log.warning("No data to save%s and no existing CSV found.", tag)
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
    # Accept optional --force flag to bypass change detection
    force_rebuild = "--force" in sys.argv

    today_str    = date.today().strftime("%Y-%m-%d")
    tomorrow_str = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    log.info("=== MLB Hitter Splits vs Starters — %s ===", today_str)
    log.info("Tomorrow-only: %s | Force rebuild: %s", tomorrow_only, force_rebuild)

    # ── Today's data ────────────────────────────────────────────────────────
    if not tomorrow_only:
        today_csv = os.path.join(DATA_DIR, "daily_starters.csv")
        if force_rebuild or starters_changed(today_str, today_csv):
            log.info("=== Rebuilding today's data — starters changed ===")
            # Clear stale splits BEFORE building so we start fresh
            clear_stale_data()
            df_today = build_daily_report(today_str)
            if not df_today.empty:
                save_report(df_today, today_str, DATA_DIR, SPLITS_DIR,
                            os.path.join(DATA_DIR, "gamelogs"), label="today")
            else:
                log.warning("No games found for today — preserving existing dashboard data.")
        else:
            log.info("=== Today's starters unchanged — skipping rebuild ===")

    # ── Tomorrow's data (every run) ─────────────────────────────────────────
    # Fetch on every run so the tomorrow tab always reflects the latest
    # announced starters — MLB posts probable starters throughout the day
    tomorrow_csv = os.path.join(TOMORROW_DIR, "daily_starters.csv")
    if force_rebuild or starters_changed(tomorrow_str, tomorrow_csv):
        log.info("=== Rebuilding tomorrow's data — starters changed ===")
        # Clear stale splits BEFORE building — same pattern as today's data
        clear_stale_data(
            target_dir    = TOMORROW_DIR,
            splits_subdir = TOMORROW_SPLITS_DIR,
            logs_subdir   = TOMORROW_LOGS_DIR,
        )
        df_tomorrow = build_tomorrow_report(tomorrow_str)
        if not df_tomorrow.empty:
            save_report(df_tomorrow, tomorrow_str, TOMORROW_DIR,
                        TOMORROW_SPLITS_DIR, TOMORROW_LOGS_DIR, label="tomorrow")
        else:
            log.warning("No games found for tomorrow — preserving existing dashboard data.")
    else:
        log.info("=== Tomorrow's starters unchanged — skipping rebuild ===")

    log.info("Done.")
