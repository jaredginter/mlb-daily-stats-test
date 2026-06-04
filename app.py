"""
app.py  —  MLB Hitter Splits vs Today's Starting Pitchers
Run locally:  streamlit run app.py
"""

import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="MLB Hitter Splits vs Starters",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ─────────────────────────────────────────────────────────────────

METRIC_COLS = {
    "xwoba":         ("xwOBA vs Pitcher",  ".3f"),
    "barrel_rate":   ("Barrel Rate",       ".1%"),
    "hard_hit_pct":  ("Hard Hit %",        ".1%"),
    "whiff_pct":     ("Whiff %",           ".1%"),
    "avg_exit_velo": ("Avg Exit Velo",     ".1f"),
    "pa":            ("PA vs Pitcher",     "d"),
    "pitches_seen":  ("Pitches Seen",      "d"),
}

# Full team name lookup by abbreviation
TEAM_NAMES = {
    "AZ":  "Diamondbacks",  "ATL": "Braves",
    "BAL": "Orioles",       "BOS": "Red Sox",
    "CHC": "Cubs",          "CWS": "White Sox",
    "CIN": "Reds",          "CLE": "Guardians",
    "COL": "Rockies",       "DET": "Tigers",
    "HOU": "Astros",        "KC":  "Royals",
    "LAA": "Angels",        "LAD": "Dodgers",
    "MIA": "Marlins",       "MIL": "Brewers",
    "MIN": "Twins",         "NYM": "Mets",
    "NYY": "Yankees",       "OAK": "Athletics",
    "PHI": "Phillies",      "PIT": "Pirates",
    "SD":  "Padres",        "SF":  "Giants",
    "SEA": "Mariners",      "STL": "Cardinals",
    "TB":  "Rays",          "TEX": "Rangers",
    "TOR": "Blue Jays",     "WSH": "Nationals",
}

# xwOBA colour scale: green (low / pitcher-friendly) → red (high / hitter-friendly)
def xwoba_color(val):
    if pd.isna(val):
        return ""
    if val < 0.280:
        return "background-color: #c6efce; color: #276221"
    if val < 0.320:
        return "background-color: #ffeb9c; color: #9c5700"
    return "background-color: #ffc7ce; color: #9c0006"


def style_splits_table(df):
    """Apply conditional formatting to the hitter splits table."""
    # Columns in the exact display order requested
    display_cols = {
        "batter_name":   "Hitter",
        "seasons":       "Seasons",
        "abs":           "ABs",
        "batting_avg":   "Batting Avg",
        "home_runs":     "Home Runs",
        "xwoba":         "xwOBA",
        "hard_hit_pct":  "HardHit%",
        "whiff_pct":     "Whiff%",
        "avg_exit_velo": "Avg EV",
    }
    available = [c for c in display_cols if c in df.columns]
    out = df[available].rename(columns=display_cols).copy()

    # Format numeric columns
    for raw, label in display_cols.items():
        if raw not in df.columns:
            continue
        if raw == "seasons":
            pass  # already a string, display as-is
        elif raw == "abs":
            out[label] = out[label].apply(lambda v: f"{int(v)}" if pd.notna(v) else "—")
        elif raw == "home_runs":
            out[label] = out[label].apply(lambda v: f"{int(v)}" if pd.notna(v) else "—")
        elif raw == "batting_avg":
            out[label] = out[label].apply(lambda v: f"{v:.3f}" if pd.notna(v) else "—")
        elif raw in ("hard_hit_pct", "whiff_pct"):
            out[label] = out[label].apply(lambda v: f"{v:.1%}" if pd.notna(v) else "—")
        elif raw == "avg_exit_velo":
            out[label] = out[label].apply(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
        elif raw == "xwoba":
            out[label] = out[label].apply(lambda v: f"{v:.3f}" if pd.notna(v) else "—")

    return out


def splits_bar_chart(df, pitcher_name, batting_team):
    """Horizontal bar chart of hitter xwOBA vs the pitcher."""
    plot_df = df[df["xwoba"].notna()].sort_values("xwoba", ascending=True)
    if plot_df.empty:
        return None

    colors = []
    for v in plot_df["xwoba"]:
        if v < 0.280:
            colors.append("#2ca02c")
        elif v < 0.320:
            colors.append("#ff7f0e")
        else:
            colors.append("#d62728")

    fig = go.Figure(go.Bar(
        x=plot_df["xwoba"],
        y=plot_df["batter_name"],
        orientation="h",
        marker_color=colors,
        text=[f"{v:.3f}" for v in plot_df["xwoba"]],
        textposition="outside",
    ))
    fig.add_vline(x=0.320, line_dash="dash", line_color="gray",
                  annotation_text="lg avg (.320)", annotation_position="top right")
    fig.update_layout(
        title=f"{batting_team} hitters — xwOBA vs {pitcher_name}",
        xaxis_title="xwOBA",
        yaxis_title="",
        height=max(300, len(plot_df) * 28 + 80),
        margin=dict(l=10, r=60, t=50, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(gridcolor="rgba(128,128,128,0.15)")
    return fig


# ── Data loading ──────────────────────────────────────────────────────────────

def get_csv_mtime(path):
    """Return file modification timestamp as a string, or empty string if missing."""
    try:
        return str(os.path.getmtime(path))
    except OSError:
        return ""


@st.cache_data(show_spinner="Loading starters…")
def load_summary(game_date_str, _mtime, data_root="data"):
    """
    Read the CSV committed by GitHub Actions.
    data_root differentiates today vs tomorrow cache entries.
    _mtime busts the cache when the file changes.
    """
    csv_path = os.path.join(data_root, "daily_starters.csv")
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_splits_cached(game_id, side, _mtime, root="data"):
    """Cache-busting wrapper for hitter splits CSVs."""
    fname = f"{game_id}_{side}_vs_{'home' if side == 'away' else 'away'}_pitcher.csv"
    path  = os.path.join(root, "hitter_splits", fname)
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_game_log_cached(pitcher_id, _mtime, root="data"):
    """Cache-busting wrapper for pitcher game log CSVs."""
    path = os.path.join(root, "gamelogs", f"{pitcher_id}_gamelog.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


# Non-cached passthrough helpers (call the cached versions with mtime)
def load_splits(game_id, side, mtime, root="data"):
    return load_splits_cached(game_id, side, mtime, root)


def load_game_log(pitcher_id, mtime, root="data"):
    return load_game_log_cached(pitcher_id, mtime, root)


def render_game_log(df, pitcher_name, season):
    """Render a styled game log table matching the screenshot layout."""
    if df.empty:
        st.caption("No game log available yet for this season.")
        return

    # Build combined OPP column BEFORE any renaming to avoid type issues
    display = df.copy()
    display["OPP"] = (
        display["home_away"].astype(str) + " " + display["opponent"].astype(str)
    )

    # Select and rename columns for display
    display = display[["date", "OPP", "result", "ip", "h", "r", "er", "hr", "bb", "k", "pitches"]].rename(columns={
        "date":    "DATE",
        "result":  "RESULT",
        "ip":      "IP",
        "h":       "H",
        "r":       "R",
        "er":      "ER",
        "hr":      "HR",
        "bb":      "BB",
        "k":       "K",
        "pitches": "P",
    })

    st.caption(f"**{season} Regular Season — {pitcher_name}**")
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
    )


# ── Run prediction model ─────────────────────────────────────────────────────

# League average baselines (2024 season)
LG_AVG_XWOBA  = 0.312
LG_AVG_FIP    = 4.00
LG_AVG_RUNS   = 4.50   # runs per team per game

# Weights tuned so league average inputs → 4.5 predicted runs
XWOBA_WEIGHT  = 18.0   # xwOBA above/below avg × this = run adjustment
FIP_WEIGHT    = 0.30   # FIP above/below avg × this = run adjustment

# Sample size thresholds for weighting the prediction
# Below FULL_TRUST_ABS the prediction is blended toward league average
# so small samples don't produce wildly skewed run totals
FULL_TRUST_ABS      = 60   # at or above this: prediction runs at full strength
HALF_TRUST_ABS      = 20   # at or above this: prediction blended 50/50 with lg avg
FULL_TRUST_HITTERS  = 7    # bonus trust if many hitters have history


def sample_size_weight(total_abs, n_hitters):
    """
    Returns a weight between 0.0 and 1.0 that scales how much the
    model's signal is trusted vs. regressing toward league average.

    - total_abs >= FULL_TRUST_ABS  → weight 1.0 (full signal)
    - total_abs ~= HALF_TRUST_ABS  → weight ~0.50
    - total_abs < 10               → weight ~0.10 (almost all league avg)

    A bonus is added when many hitters have history, since breadth of
    matchup data is independently meaningful even if AB count is low.
    """
    if total_abs is None or total_abs <= 0:
        return 0.10

    # Logarithmic scale: diminishing returns as ABs grow
    import math
    base_weight = min(1.0, math.log1p(total_abs) / math.log1p(FULL_TRUST_ABS))

    # Hitter breadth bonus (up to +0.10)
    hitter_bonus = 0.0
    if n_hitters is not None and n_hitters >= FULL_TRUST_HITTERS:
        hitter_bonus = 0.10

    return min(1.0, base_weight + hitter_bonus)


def predict_runs(avg_xwoba, fip_vs_team, splits_df, total_abs=None, n_hitters=None):
    """
    Predict runs scored by the batting team using a weighted model:
      - xwOBA vs pitcher (strongest signal)
      - FIP vs this team (pitcher quality signal)
      - Avg HardHit% and Whiff% from the hitter table (secondary signals)
      - Sample size weight (total ABs + hitter count) scales prediction
        toward league average when history is sparse

    Returns (predicted_runs, confidence_label, confidence_color, inputs_used, sample_weight)
    """
    if avg_xwoba is None and fip_vs_team is None:
        return None, None, None, [], 0.0

    # Compute sample size weight
    sw = sample_size_weight(total_abs, n_hitters)

    raw_prediction = LG_AVG_RUNS
    inputs_used    = []

    # Signal 1: xwOBA vs pitcher (most reliable)
    if avg_xwoba is not None and not pd.isna(avg_xwoba):
        xwoba_adj    = (float(avg_xwoba) - LG_AVG_XWOBA) * XWOBA_WEIGHT
        raw_prediction += xwoba_adj
        inputs_used.append("xwOBA")

    # Signal 2: FIP vs this team (pitcher quality)
    if fip_vs_team is not None and not pd.isna(fip_vs_team):
        fip_adj      = (float(fip_vs_team) - LG_AVG_FIP) * FIP_WEIGHT
        raw_prediction += fip_adj
        inputs_used.append("FIP")

    # Signal 3: avg HardHit% and Whiff% from the splits table
    if splits_df is not None and not splits_df.empty:
        if "hard_hit_pct" in splits_df.columns:
            hh = splits_df["hard_hit_pct"].dropna().mean()
            if not pd.isna(hh):
                raw_prediction += (hh - 0.38) * 4.0
                inputs_used.append("HardHit%")
        if "whiff_pct" in splits_df.columns:
            wp = splits_df["whiff_pct"].dropna().mean()
            if not pd.isna(wp):
                raw_prediction -= (wp - 0.24) * 4.0
                inputs_used.append("Whiff%")

    # Blend prediction toward league average based on sample size weight
    # sw=1.0 → full model signal; sw=0.1 → 90% league avg, 10% model signal
    blended = (sw * raw_prediction) + ((1.0 - sw) * LG_AVG_RUNS)

    # Floor / ceiling
    blended = max(1.0, min(blended, 12.0))

    # Confidence: combination of signal count AND sample size weight
    if sw >= 0.80 and len(inputs_used) >= 3:
        conf_label = "High confidence"
        conf_color = "#2ca02c"
    elif sw >= 0.50 and len(inputs_used) >= 2:
        conf_label = "Medium confidence"
        conf_color = "#ff7f0e"
    elif sw >= 0.25:
        conf_label = "Low confidence"
        conf_color = "#d62728"
    else:
        conf_label = "Very low — small sample"
        conf_color = "#9467bd"

    return round(blended, 1), conf_label, conf_color, inputs_used, round(sw, 2)


def run_prediction_badge(runs, conf_label, conf_color, team, inputs_used, sample_weight=1.0, total_abs=None, n_hitters=None):
    """Render a styled predicted runs badge."""
    signals = ", ".join(inputs_used) if inputs_used else "insufficient data"
    sw_pct  = int(round(sample_weight * 100))
    abs_str = f"{total_abs} career ABs" if total_abs else "unknown ABs"
    hit_str = f"{n_hitters} hitters" if n_hitters else "unknown hitters"
    sample_note = f"Sample weight: {sw_pct}% ({abs_str} · {hit_str})"
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border: 1px solid {conf_color};
            border-radius: 10px;
            padding: 12px 18px;
            margin: 6px 0 10px 0;
            display: flex;
            align-items: center;
            justify-content: space-between;
        ">
            <div>
                <div style="font-size:0.75rem;color:#aaa;margin-bottom:2px;">
                    📊 Predicted runs — {team}
                </div>
                <div style="font-size:2rem;font-weight:700;color:white;line-height:1.1;">
                    {runs}
                </div>
                <div style="font-size:0.7rem;color:#888;margin-top:2px;">
                    Based on: {signals}
                </div>
                <div style="font-size:0.68rem;color:#666;margin-top:2px;">
                    {sample_note}
                </div>
            </div>
            <div style="text-align:right;">
                <div style="
                    font-size:0.7rem;
                    font-weight:600;
                    color:{conf_color};
                    border:1px solid {conf_color};
                    border-radius:20px;
                    padding:3px 10px;
                ">
                    {conf_label}
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("⚾ MLB Starter Splits")

    # Today / Tomorrow toggle
    tomorrow_available = os.path.exists(os.path.join("data", "tomorrow", "daily_starters.csv"))
    day_options        = ["Today", "Tomorrow"] if tomorrow_available else ["Today"]
    selected_day       = st.radio(
        "Game day",
        day_options,
        horizontal=True,
        help="Tomorrow's data is fetched each evening at 9 PM CST once MLB posts probable starters."
    )
    is_tomorrow   = (selected_day == "Tomorrow")
    data_root     = os.path.join("data", "tomorrow") if is_tomorrow else "data"
    selected_date = date.today() + timedelta(days=1) if is_tomorrow else date.today()

    st.caption("Shows each opposing hitter's Statcast stats vs. today's starter — season-to-date.")
    st.divider()
    show_chart      = st.toggle("Show xwOBA chart", value=True)
    show_table      = st.toggle("Show hitter table", value=True)
    show_gamelog    = st.toggle("Show pitcher game log", value=True)
    show_prediction = st.toggle("Show run prediction", value=True)
    st.divider()
    if st.button("🔄 Force refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    ts_path = os.path.join(data_root, "last_updated.txt")
    if os.path.exists(ts_path):
        with open(ts_path) as f:
            last_updated = f.read().strip()
        st.caption(f"Data last updated: {last_updated}")
    else:
        st.caption("Data update time unknown")

    if not tomorrow_available:
        st.caption("Tomorrow's data posts after 9 PM CST once MLB announces starters.")

# ── Main ──────────────────────────────────────────────────────────────────────

st.title(f"Hitter Splits vs Starters — {selected_date.strftime('%A, %B %d %Y')}")
st.caption("Each panel shows the **opposing lineup's** career Statcast numbers vs. that starting pitcher (all seasons since 2015).")

date_str      = selected_date.strftime("%Y-%m-%d")
csv_path      = os.path.join(data_root, "daily_starters.csv")
current_mtime = get_csv_mtime(csv_path)
summary       = load_summary(date_str, current_mtime, data_root=data_root)

if summary.empty:
    st.warning("No games or probable starters found for this date. "
               "MLB usually posts starters by mid-morning — try again later.")
    st.stop()

st.caption(f"{len(summary)} game{'s' if len(summary) != 1 else ''} today")

# ── Game cards ────────────────────────────────────────────────────────────────

for _, game in summary.iterrows():
    matchup   = game.get("matchup", "Game")
    game_id   = game["game_id"]
    game_time_raw = game.get("game_time", "")
    try:
        gt = datetime.fromisoformat(game_time_raw.replace("Z", "+00:00"))
        game_time_str = gt.astimezone().strftime("%I:%M %p %Z")
    except Exception:
        game_time_str = ""

    with st.expander(f"**{matchup}** — {game_time_str}", expanded=True):

        # ── Two panels: away pitcher | home pitcher ───────────────────────────
        # Panel 1: Home pitcher vs Away lineup
        # Panel 2: Away pitcher vs Home lineup

        panels = [
            {
                "pitcher_name": game.get("home_pitcher_name", "TBD"),
                "pitcher_side": "home",
                "batting_team": game.get("away_team", ""),
                "splits_side":  "away",   # away hitters vs home pitcher
                "hitters_with_history": game.get("away_hitters_with_history"),
                "lineup_avg_xwoba":     game.get("away_lineup_avg_xwoba"),
            },
            {
                "pitcher_name": game.get("away_pitcher_name", "TBD"),
                "pitcher_side": "away",
                "batting_team": game.get("home_team", ""),
                "splits_side":  "home",   # home hitters vs away pitcher
                "hitters_with_history": game.get("home_hitters_with_history"),
                "lineup_avg_xwoba":     game.get("home_lineup_avg_xwoba"),
            },
        ]

        # ── Combined game total prediction banner ────────────────────────
        if show_prediction:
            away_xwoba = game.get("away_lineup_avg_xwoba")
            home_xwoba = game.get("home_lineup_avg_xwoba")
            away_fip   = game.get("home_pitcher_fip_vs_opp")  # home pitcher faces away batters
            home_fip   = game.get("away_pitcher_fip_vs_opp")  # away pitcher faces home batters

            away_total_abs = game.get("away_total_abs")
            home_total_abs = game.get("home_total_abs")
            away_n         = game.get("away_hitters_with_history")
            home_n         = game.get("home_hitters_with_history")

            away_pred, _, _, _, _ = predict_runs(away_xwoba, away_fip, None,
                                                  total_abs=away_total_abs, n_hitters=away_n)
            home_pred, _, _, _, _ = predict_runs(home_xwoba, home_fip, None,
                                                  total_abs=home_total_abs, n_hitters=home_n)

            if away_pred is not None and home_pred is not None:
                total = round(away_pred + home_pred, 1)
                away_team = game.get("away_team", "Away")
                home_team = game.get("home_team", "Home")
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #0d1b2a 0%, #1b263b 100%);
                        border: 1px solid #415a77;
                        border-radius: 10px;
                        padding: 10px 18px;
                        margin-bottom: 12px;
                        display: flex;
                        align-items: center;
                        gap: 24px;
                    ">
                        <div style="color:#aaa;font-size:0.75rem;white-space:nowrap;">
                            🎯 Predicted total
                        </div>
                        <div style="font-size:1.6rem;font-weight:700;color:white;">
                            {total}
                        </div>
                        <div style="color:#aaa;font-size:0.8rem;">
                            {away_team} <span style="color:#778da9;">~{away_pred}</span>
                            &nbsp;·&nbsp;
                            {home_team} <span style="color:#778da9;">~{home_pred}</span>
                        </div>
                        <div style="margin-left:auto;font-size:0.7rem;color:#778da9;">
                            Model: xwOBA + FIP
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        col_left, col_div, col_right = st.columns([5, 0.2, 5])

        for col, panel in zip([col_left, col_right], panels):
            with col:
                pitcher = panel["pitcher_name"]
                batting = panel["batting_team"]

                full_team_name = TEAM_NAMES.get(batting, batting)
                st.subheader(f"{full_team_name} vs {pitcher}")
                st.caption(f"{full_team_name} stats vs {pitcher}.")

                if not pitcher or pitcher == "TBD":
                    st.info("Starter not yet announced.")
                    continue

                n = panel["hitters_with_history"]
                avg_xwoba = panel["lineup_avg_xwoba"]

                # FIP key depends on which side is pitching
                fip_key = (
                    "home_pitcher_fip_vs_opp"
                    if panel["pitcher_side"] == "home"
                    else "away_pitcher_fip_vs_opp"
                )
                fip_val = game.get(fip_key)

                # Total AB threshold for FIP reliability (AB-based, not hitter count)
                FIP_UNRELIABLE_ABS = 20   # red/strong warning below this
                FIP_CAUTION_ABS    = 40   # yellow caution below this

                # Pull total career ABs for this side from the summary row
                total_abs_key = (
                    "away_total_abs"
                    if panel["pitcher_side"] == "home"
                    else "home_total_abs"
                )
                total_abs = game.get(total_abs_key)
                try:
                    total_abs = int(total_abs) if total_abs is not None else 0
                except (ValueError, TypeError):
                    total_abs = 0

                if n is not None and n > 0:
                    mc1, mc2, mc3 = st.columns(3)
                    mc1.metric("Hitters with history", int(n))
                    mc2.metric("Lineup avg xwOBA", f"{avg_xwoba:.3f}" if avg_xwoba else "—")

                    fip_display = (
                        f"{fip_val:.2f}"
                        if fip_val is not None and str(fip_val) != "nan"
                        else "—"
                    )
                    mc3.metric(
                        "FIP vs this team",
                        fip_display,
                        help="{pitcher}FIP vs {full_team_name} (career). "
                             "Lower is better for the pitcher (opposite side). Higher is better for {full_team_name}. Scale: <3.20 elite, 3.20–3.79 good, "
                             "3.80–4.19 average, 4.20–4.79 below avg, 5.00+ poor."
                    )

                    # Sample size warning based on total career ABs vs this pitcher
                    if total_abs < FIP_UNRELIABLE_ABS:
                        st.error(
                            f"🚨 **FIP unreliable — very small sample.** "
                            f"Only **{total_abs} total career ABs** across {int(n)} hitters vs this pitcher. "
                            f"With fewer than {FIP_UNRELIABLE_ABS} ABs, a single home run can swing FIP "
                            f"by several points. Disregard FIP for this matchup."
                        )
                    elif total_abs < FIP_CAUTION_ABS:
                        st.warning(
                            f"⚠️ **Small sample — interpret FIP with caution.** "
                            f"**{total_abs} total career ABs** across {int(n)} hitters vs this pitcher. "
                            f"FIP is most reliable with 40+ total ABs."
                        )
                else:
                    st.info("No head-to-head Statcast history found for this matchup yet "
                            "(common early in the season or for new pitchers).")
                    continue

                # ── Predicted runs badge ─────────────────────────────────
                if show_prediction:
                    _splits_preview = load_splits(game_id, panel["splits_side"], current_mtime, root=data_root)
                    pred_runs, conf_label, conf_color, inputs_used, sw = predict_runs(
                        avg_xwoba, fip_val, _splits_preview,
                        total_abs=total_abs, n_hitters=int(n) if n else None
                    )
                    if pred_runs is not None:
                        run_prediction_badge(
                            pred_runs, conf_label, conf_color, batting, inputs_used,
                            sample_weight=sw, total_abs=total_abs,
                            n_hitters=int(n) if n else None
                        )

                splits_df = load_splits(game_id, panel["splits_side"], current_mtime, root=data_root)

                if splits_df.empty:
                    st.warning("Split data not found — try running the fetch script.")
                    continue

                if show_chart:
                    fig = splits_bar_chart(splits_df, pitcher, batting)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True,
                                        key=f"chart_{game_id}_{panel['splits_side']}")

                if show_table:
                    display = style_splits_table(splits_df)
                    st.dataframe(
                        display,
                        use_container_width=True,
                        hide_index=True,
                        key=f"table_{game_id}_{panel['splits_side']}",
                    )

                # ── Game log ─────────────────────────────────────────────
                pitcher_id = game.get(f"{panel['pitcher_side']}_pitcher_id")
                if pitcher_id and show_gamelog:
                    st.markdown("---")
                    gl_df = load_game_log(pitcher_id, current_mtime, root=data_root)
                    render_game_log(gl_df, pitcher, selected_date.year)

        with col_div:
            st.markdown(
                "<div style='text-align:center;font-size:1.4rem;"
                "padding-top:3rem;color:#888'>vs</div>",
                unsafe_allow_html=True,
            )

st.divider()
st.caption(
    "Data: Baseball Savant (Statcast) via pybaseball · MLB Stats API · "
    "Career splits cover all Statcast seasons (2015–present) vs. today's starter."
)
