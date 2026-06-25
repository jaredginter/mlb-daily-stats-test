"""
app.py  —  MLB Hitter Splits vs Today's Starting Pitchers
Run locally:  streamlit run app.py
"""

import contextlib
import io
import os
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import statsmodels.api as sm
import streamlit as st
from statsmodels.stats.outliers_influence import variance_inflation_factor

try:
    from mlb_regression_analysis import (
        fetch_batting, fetch_runs_per_game, build_dataset,
        run_ols, run_ridge_lasso, FEATURES,
    )
    _REGRESSION_AVAILABLE = True
except ImportError:
    _REGRESSION_AVAILABLE = False

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


def fip_xwoba_quadrant(avg_xwoba, fip, pitcher_name, batting_team, pitching_team,
                       total_abs=None, n_hitters=None):
    """
    3×3 heatmap of FIP zones (cols) vs xwOBA zones (rows).

    FIP zones  : Low <3.80 | Avg 3.80–4.80 | High >4.80
    xwOBA zones: Low <0.300 | Avg 0.300–0.340 | High >0.340

    Active cell is highlighted; scenario label + detail returned for the banner.
    Sample weight drives cell opacity and the reliability metric card.

    Returns (fig, label, detail, color, emoji, sw) or None if inputs invalid.
    """
    # ── Zone boundaries ──────────────────────────────────────────────────
    FIP_LOW  = 3.80
    FIP_HIGH = 4.80
    XW_LOW   = 0.300
    XW_HIGH  = 0.340

    try:
        avg_xwoba = float(avg_xwoba)
        fip       = float(fip)
    except (TypeError, ValueError):
        return None
    if avg_xwoba != avg_xwoba or fip != fip:
        return None

    # ── Sample weight ────────────────────────────────────────────────────
    sw      = sample_size_weight(total_abs, n_hitters)
    sw_pct  = int(round(sw * 100))
    abs_str = f"{total_abs} career ABs" if total_abs else "unknown ABs"
    hit_str = f"{n_hitters} hitters"    if n_hitters else "unknown hitters"

    # ── Classify into zones ──────────────────────────────────────────────
    # fip_col: 0=Low, 1=Avg, 2=High
    fip_col = 0 if fip < FIP_LOW else (1 if fip <= FIP_HIGH else 2)
    # xw_row: 0=Low (bottom), 1=Avg (middle), 2=High (top)
    xw_row  = 0 if avg_xwoba < XW_LOW else (1 if avg_xwoba <= XW_HIGH else 2)

    # ── 3×3 scenario table [xw_row][fip_col] ────────────────────────────
    # Colors: green=#43a047, yellow=#f9a825, gray=#607d8b
    SCENARIOS = {
        # (xw_row, fip_col): (short_label, color, emoji, detail_fn)
        (2, 0): (
            f"{pitching_team} Pitching Holds Edge",
            "#81c784", "🟢",
            f"Contested matchup — {batting_team} hitters make strong contact "
            f"(xwOBA {avg_xwoba:.3f}) but {pitcher_name} has been elite (FIP {fip:.2f}). "
            f"Hitters have a puncher's chance but the pitcher holds the edge."
        ),
        (2, 1): (
            f"Slight {batting_team} Offensive Edge",
            "#81c784", "🟢",
            f"Slight offensive lean — {batting_team} hitters are making good contact "
            f"(xwOBA {avg_xwoba:.3f}) against an average FIP pitcher ({fip:.2f}). "
            f"Mild edge to the offense but far from a slam dunk."
        ),
        (2, 2): (
            f"{batting_team} Offense Strongly Favored",
            "#43a047", "🟢",
            f"High scoring game likely — {batting_team} hitters are squaring up {pitcher_name} "
            f"(xwOBA {avg_xwoba:.3f}) and the pitcher has struggled vs this lineup (FIP {fip:.2f}). "
            f"Strong indicator to stack the {batting_team} lineup."
        ),
        (1, 0): (
            f"{pitching_team} Pitching Holds Edge",
            "#81c784", "🟢",
            f"Pitcher holds the edge — {batting_team} hitters show average contact quality "
            f"(xwOBA {avg_xwoba:.3f}) while {pitcher_name} has been elite (FIP {fip:.2f}). "
            f"Low run environment expected."
        ),
        (1, 1): (
            f"Toss-Up — {batting_team} vs {pitching_team}",
            "#607d8b", "⚪",
            f"True toss-up — both sides are average. {batting_team} hitters at xwOBA {avg_xwoba:.3f} "
            f"vs {pitcher_name}'s FIP of {fip:.2f}. No clear edge — lean on other factors."
        ),
        (1, 2): (
            f"Slight {batting_team} Offensive Edge",
            "#81c784", "🟢",
            f"Slight offensive lean — average contact quality (xwOBA {avg_xwoba:.3f}) meets a "
            f"struggling pitcher (FIP {fip:.2f}). Mild advantage to {batting_team} but not a "
            f"strong signal on its own."
        ),
        (0, 0): (
            f"{pitching_team} Pitching Strongly Favored",
            "#43a047", "🟢",
            f"Low scoring game likely — {pitcher_name} dominates this matchup. "
            f"{batting_team} hitters have weak contact quality (xwOBA {avg_xwoba:.3f}) "
            f"and the pitcher's FIP is elite ({fip:.2f}). Pitcher is firmly in control."
        ),
        (0, 1): (
            f"{pitching_team} Pitching Holds Edge",
            "#81c784", "🟢",
            f"Pitcher holds the edge — {batting_team} hitters are struggling (xwOBA {avg_xwoba:.3f}) "
            f"against an average FIP pitcher ({fip:.2f}). Lean toward a quieter offensive game."
        ),
        (0, 2): (
            f"Mixed Signal — {batting_team} vs {pitching_team}",
            "#f9a825", "🟡",
            f"Murky matchup — {pitcher_name} is walk- or homer-prone (FIP {fip:.2f}) but "
            f"{batting_team} hitters haven't made strong contact (xwOBA {avg_xwoba:.3f}). "
            f"Unpredictable — lean on other factors before committing."
        ),
    }

    base_label, color, emoji, detail = SCENARIOS[(xw_row, fip_col)]

    # ── Sample-size override ─────────────────────────────────────────────
    if sw < 0.50:
        label         = "Inconclusive — very small sample"
        display_color = "#9467bd"
        display_emoji = "⚪"
        qualifier_note = (
            f"Only {abs_str} across {hit_str} — not enough history to read this "
            f"matchup. Heatmap position shown for reference only."
        )
    else:
        label         = base_label
        display_color = color
        display_emoji = emoji
        qualifier_note = f"{abs_str} · {hit_str} · {sw_pct}% sample reliability."

    # ── Build heatmap ────────────────────────────────────────────────────
    # Grid: 3 cols (FIP) × 3 rows (xwOBA), rendered as SVG-style rectangles
    # in a Plotly figure with axes suppressed.

    # Cell display text (short labels for inside each cell)
    CELL_TEXT = {
        (2, 0): ("Pitcher", "Holds Edge"),
        (2, 1): ("Slight Off.", "Edge"),
        (2, 2): ("Offense", "Strongly Fav."),
        (1, 0): ("Pitcher", "Holds Edge"),
        (1, 1): ("Toss-Up", ""),
        (1, 2): ("Slight Off.", "Edge"),
        (0, 0): ("Pitcher", "Strongly Fav."),
        (0, 1): ("Pitcher", "Holds Edge"),
        (0, 2): ("Mixed", "Signal"),
    }

    # Cell fill colors (dimmed versions; active cell gets full opacity)
    CELL_COLORS = {
        (2, 0): "rgba(129,199,132,{a})",   # light green — Pitcher Holds Edge
        (2, 1): "rgba(129,199,132,{a})",   # light green — Slight Offensive Edge
        (2, 2): "rgba(67,160,71,{a})",     # green — Offense Strongly Favored
        (1, 0): "rgba(129,199,132,{a})",   # light green — Pitcher Holds Edge
        (1, 1): "rgba(96,125,139,{a})",    # gray — Toss-Up
        (1, 2): "rgba(129,199,132,{a})",   # light green — Slight Offensive Edge
        (0, 0): "rgba(67,160,71,{a})",     # green — Pitcher Strongly Favored
        (0, 1): "rgba(129,199,132,{a})",   # light green — Pitcher Holds Edge
        (0, 2): "rgba(249,168,37,{a})",    # yellow — Mixed Signal
    }

    fig = go.Figure()

    CELL_W = 1.0   # each cell is 1 unit wide/tall in plot space
    GAP    = 0.04  # gap between cells

    col_labels = [f"Low FIP\n(<{FIP_LOW})", f"Avg FIP\n({FIP_LOW}–{FIP_HIGH})", f"High FIP\n(>{FIP_HIGH})"]
    row_labels = [f"Low xwOBA\n(<{XW_LOW})", f"Avg xwOBA\n({XW_LOW}–{XW_HIGH})", f"High xwOBA\n(>{XW_HIGH})"]

    for row in range(3):
        for col in range(3):
            active  = (row == xw_row and col == fip_col)
            alpha   = "0.85" if active else "0.18"
            fill    = CELL_COLORS[(row, col)].format(a=alpha)
            x0 = col * (CELL_W + GAP)
            x1 = x0 + CELL_W
            y0 = row * (CELL_W + GAP)
            y1 = y0 + CELL_W
            cx = (x0 + x1) / 2
            cy = (y0 + y1) / 2

            # Cell background
            fig.add_shape(
                type="rect", x0=x0, x1=x1, y0=y0, y1=y1,
                fillcolor=fill,
                line=dict(
                    color="white" if active else "rgba(255,255,255,0.10)",
                    width=3 if active else 1,
                ),
                layer="below",
            )

            # Active cell glow — extra outer border
            if active:
                fig.add_shape(
                    type="rect",
                    x0=x0 - 0.02, x1=x1 + 0.02,
                    y0=y0 - 0.02, y1=y1 + 0.02,
                    fillcolor="rgba(0,0,0,0)",
                    line=dict(color=display_color, width=2),
                    layer="above",
                )

            # Cell text
            line1, line2 = CELL_TEXT[(row, col)]
            txt_color    = "white" if active else "rgba(255,255,255,0.40)"
            txt_size     = 10 if active else 8
            txt_weight   = "bold" if active else "normal"
            cell_text    = f"<b>{line1}</b><br>{line2}" if active else f"{line1}<br>{line2}"

            fig.add_annotation(
                x=cx, y=cy,
                text=cell_text,
                showarrow=False,
                font=dict(size=txt_size, color=txt_color),
                align="center",
                xanchor="center",
                yanchor="middle",
            )

    # Column headers (FIP labels) — above top row
    for col, lbl in enumerate(col_labels):
        cx = col * (CELL_W + GAP) + CELL_W / 2
        fig.add_annotation(
            x=cx, y=3 * (CELL_W + GAP) + 0.05,
            text=lbl.replace("\n", "<br>"),
            showarrow=False,
            font=dict(size=8, color="rgba(255,255,255,0.55)"),
            align="center", xanchor="center", yanchor="bottom",
        )

    # Row headers (xwOBA labels) — left of leftmost col
    for row, lbl in enumerate(row_labels):
        cy = row * (CELL_W + GAP) + CELL_W / 2
        fig.add_annotation(
            x=-0.08, y=cy,
            text=lbl.replace("\n", "<br>"),
            showarrow=False,
            font=dict(size=8, color="rgba(255,255,255,0.55)"),
            align="right", xanchor="right", yanchor="middle",
        )

    total_span = 3 * CELL_W + 2 * GAP

    fig.update_layout(
        height=310,
        margin=dict(l=90, r=10, t=55, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white", size=10),
        xaxis=dict(
            range=[-0.05, total_span + 0.05],
            showgrid=False, zeroline=False,
            showticklabels=False, visible=False,
        ),
        yaxis=dict(
            range=[-0.05, total_span + 0.55],
            showgrid=False, zeroline=False,
            showticklabels=False, visible=False,
            scaleanchor="x", scaleratio=1,
        ),
        showlegend=False,
        title=dict(
            text=f"Matchup Heatmap — FIP vs xwOBA  "
                 f"<span style='font-size:11px;color:rgba(255,255,255,0.45);'>"
                 f"(FIP {fip:.2f} · xwOBA {avg_xwoba:.3f})</span>",
            font=dict(size=12),
            x=0.5, xanchor="center",
        ),
    )

    # Invisible scatter so hover works on the active cell
    cx_active = fip_col * (CELL_W + GAP) + CELL_W / 2
    cy_active = xw_row  * (CELL_W + GAP) + CELL_W / 2
    fig.add_trace(go.Scatter(
        x=[cx_active], y=[cy_active],
        mode="markers",
        marker=dict(size=1, opacity=0),
        hovertemplate=(
            f"<b>{display_emoji} {label}</b><br>"
            f"FIP vs {batting_team}: <b>{fip:.2f}</b><br>"
            f"Lineup avg xwOBA: <b>{avg_xwoba:.3f}</b><br>"
            f"Sample weight: <b>{sw_pct}%</b> ({abs_str} · {hit_str})<br>"
            f"<br><i style='color:#ccc'>{qualifier_note}</i>"
            "<extra></extra>"
        ),
        showlegend=False,
    ))

    return fig, label, detail, display_color, display_emoji, sw


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


# ── Regression data loader ────────────────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_regression_data(seasons_tuple):
    """
    Fetch and join team batting + runs data for regression.
    Cached 24 hours — the network calls are the slow step.
    Raises on failure so the caller can surface the real error.
    """
    if not _REGRESSION_AVAILABLE:
        raise RuntimeError("mlb_regression_analysis module not importable.")
    seasons = list(seasons_tuple)
    batting_df = fetch_batting(seasons)
    runs_df    = fetch_runs_per_game(seasons)
    return build_dataset(batting_df, runs_df)


@st.cache_data(ttl=300)  # re-checks the live MLB API every 5 minutes
def check_live_starters(game_date_str: str) -> dict:
    """
    Lightweight check against the live MLB schedule endpoint.
    Returns {game_id: (home_pitcher_name, away_pitcher_name)} from the API.
    No pybaseball, no Statcast — just the schedule endpoint, so it's fast.
    TTL of 300 s means Streamlit re-polls every 5 minutes automatically.
    """
    import requests as _req
    try:
        url  = "https://statsapi.mlb.com/api/v1/schedule"
        r    = _req.get(
            url,
            params={"sportId": 1, "date": game_date_str, "hydrate": "probablePitcher,team"},
            timeout=8,
        )
        r.raise_for_status()
        dates = r.json().get("dates", [])
    except Exception:
        return {}

    live = {}
    for date_entry in dates:
        for game in date_entry.get("games", []):
            gid       = str(game["gamePk"])
            home_name = (game.get("teams", {}).get("home", {})
                             .get("probablePitcher", {}).get("fullName", "TBD"))
            away_name = (game.get("teams", {}).get("away", {})
                             .get("probablePitcher", {}).get("fullName", "TBD"))
            live[gid] = (home_name, away_name)
    return live


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

import math

# League average baselines (2024 season)
LG = {
    "runs":     4.50,
    "xwoba":    0.312,
    "fip":      4.00,
    "hard_hit": 0.380,
    "whiff":    0.240,
    "woba":     0.312,
    "wrc_plus": 100.0,
    "obp":      0.318,
    "ops_plus": 100.0,
    "barrel":   0.080,
    "k_pct":    0.222,
    "bb_pct":   0.083,
    "babip":    0.296,
}

# AB score saturates at 80 ABs (65% of weight)
# Hitter score saturates at 9 hitters (35% of weight)
FULL_TRUST_ABS     = 80
FULL_TRUST_HITTERS = 9


def safe_float(val, default):
    try:
        v = float(val)
        return default if (v != v) else v  # nan check
    except (TypeError, ValueError):
        return default


def sample_size_weight(total_abs, n_hitters):
    """
    Dual-component reliability score 0.0–1.0.

    Two factors scored independently on a log scale, then blended:
      - AB score     (65% weight): how many total career ABs vs this pitcher
      - Hitter score (35% weight): how many roster hitters have history

    Both scale continuously — more ABs and more hitters each independently
    increase reliability rather than a binary bonus flip.

    Saturation points: 80 ABs = full AB score, 9 hitters = full hitter score.
    Minimum floor of 0.05 when no data exists.
    """
    if total_abs is None or total_abs <= 0:
        return 0.05

    ab_score = min(1.0, math.log1p(total_abs) / math.log1p(FULL_TRUST_ABS))

    if n_hitters is not None and n_hitters > 0:
        hitter_score = min(1.0, math.log1p(n_hitters) / math.log1p(FULL_TRUST_HITTERS))
    else:
        hitter_score = 0.0

    return min(1.0, (ab_score * 0.65) + (hitter_score * 0.35))


def predict_runs(avg_xwoba, fip_vs_team, splits_df,
                 total_abs=None, n_hitters=None, team_off=None):
    """
    Two-layer run prediction model:

    Layer 1 — Team season offense (40% weight):
      wOBA 10%, wRC+ 8%, OBP 6%, OPS+ 4%, Barrel% 4%,
      HardHit% 3%, K% 3%, BB% 1%, BABIP 1%

    Layer 2 — vs pitcher career splits (60% weight, sample-size adjusted):
      xwOBA vs pitcher 25%, FIP vs pitcher 12%,
      HardHit% vs pitcher 10%, Whiff% vs pitcher 8%,
      scaled toward team stats when career AB sample is thin

    Returns (predicted_runs, conf_label, conf_color, inputs_used, sample_weight)
    """
    if avg_xwoba is None and fip_vs_team is None and not team_off:
        return None, None, None, [], 0.0

    inputs_used = []
    BASE        = LG["runs"]

    # ── Layer 1: team season offense (always available) ──────────────────
    team_adj = 0.0
    if team_off:
        def tadj(key, weight, lg_key=None):
            val = safe_float(team_off.get(key), LG.get(lg_key or key, 0))
            return (val - LG.get(lg_key or key, val)) * weight

        team_adj += tadj("woba",     12.0)
        team_adj += tadj("wrc_plus",  0.012)   # per point above 100
        team_adj += tadj("obp",       8.0)
        team_adj += tadj("ops_plus",  0.008)
        team_adj += tadj("barrel",   10.0)
        team_adj += tadj("hard_hit",  4.0)
        team_adj -= tadj("k_pct",     6.0)
        team_adj += tadj("bb_pct",    5.0)
        team_adj += tadj("babip",     4.0)
        team_adj *= 0.40
        inputs_used += ["wOBA","wRC+","OBP","OPS+","Barrel%","HardHit%","K%","BB%","BABIP"]

    # ── Layer 2: vs-pitcher career splits (sample-size weighted) ─────────
    sw         = sample_size_weight(total_abs, n_hitters)
    splits_adj = 0.0

    if avg_xwoba is not None and not pd.isna(avg_xwoba):
        splits_adj += (safe_float(avg_xwoba, LG["xwoba"]) - LG["xwoba"]) * 18.0
        inputs_used.append("xwOBA vs pitcher")

    if fip_vs_team is not None and not pd.isna(fip_vs_team):
        splits_adj += (safe_float(fip_vs_team, LG["fip"]) - LG["fip"]) * 0.30
        inputs_used.append("FIP vs pitcher")

    if splits_df is not None and not splits_df.empty:
        if "hard_hit_pct" in splits_df.columns:
            hh = splits_df["hard_hit_pct"].dropna().mean()
            if not pd.isna(hh):
                splits_adj += (hh - LG["hard_hit"]) * 4.0
                inputs_used.append("HardHit% vs pitcher")
        if "whiff_pct" in splits_df.columns:
            wp = splits_df["whiff_pct"].dropna().mean()
            if not pd.isna(wp):
                splits_adj -= (wp - LG["whiff"]) * 4.0
                inputs_used.append("Whiff% vs pitcher")

    # Scale splits layer by sample size — when thin, lean on team offense
    splits_adj *= (sw * 0.60)

    raw     = BASE + team_adj + splits_adj
    blended = max(1.0, min(12.0, raw))

    # Confidence: needs both layers + good sample to hit High
    n_sig = len(inputs_used)
    if sw >= 0.80 and n_sig >= 6:
        conf_label = "High confidence"
        conf_color = "#2ca02c"
    elif sw >= 0.50 and n_sig >= 4:
        conf_label = "Medium confidence"
        conf_color = "#ff7f0e"
    elif n_sig >= 2:
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
    view = st.radio(
        "View",
        ["⚾ Today's Splits", "📊 Regression Analysis"],
        horizontal=True,
        label_visibility="collapsed",
    )
    st.divider()

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
    show_prediction = st.toggle("Show run prediction", value=True)
    show_quadrant   = st.toggle("Show FIP vs xwOBA quadrant", value=True)
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

# ── Regression view (renders and exits via st.stop()) ────────────────────────
if view == "📊 Regression Analysis":
    st.title("📊 Team Regression Analysis")
    st.caption("OLS regression predicting team runs per game from sabermetric batting features.")

    if not _REGRESSION_AVAILABLE:
        st.error(
            "Regression module not available. "
            "Install `pybaseball`, `statsmodels`, and `scikit-learn`, then restart Streamlit."
        )
        st.stop()

    # Controls
    ctrl1, ctrl2 = st.columns([3, 1])
    with ctrl1:
        reg_seasons = st.multiselect(
            "Seasons",
            [2018, 2019, 2021, 2022, 2023, 2024],
            default=[2018, 2019, 2021, 2022, 2023, 2024],
            help="2020 excluded by default — 60-game season distorts metrics.",
        )
    with ctrl2:
        st.write("")
        if st.button("🔄 Clear Cache", use_container_width=True,
                     help="Force re-fetch from FanGraphs / Baseball Reference."):
            fetch_regression_data.clear()
            st.rerun()

    if len(reg_seasons) < 2:
        st.warning("Select at least 2 seasons to run the regression.")
        st.stop()

    with st.spinner("Fetching data… (30–60 s on first run, then cached for 24 h)"):
        try:
            reg_dataset = fetch_regression_data(tuple(sorted(reg_seasons)))
        except Exception as _reg_err:
            import traceback as _tb
            st.error(f"**Data fetch failed:** {_reg_err}")
            with st.expander("Full traceback"):
                st.code(_tb.format_exc())
            st.caption("Try clicking **Clear Cache** above, then reload. "
                       "If the error mentions FanGraphs or Baseball Reference, "
                       "the external site may be temporarily rate-limiting requests.")
            st.stop()

    if reg_dataset.empty:
        st.error(
            "Dataset is empty after fetching — the FanGraphs/Baseball Reference "
            "data joined to 0 rows. Try clearing the cache and reloading."
        )
        st.stop()

    # Fit model (fast — data fetch is the slow step)
    with contextlib.redirect_stdout(io.StringIO()):
        ols_model, reg_X, reg_y = run_ols(reg_dataset)

    # Model summary cards
    st.subheader("Model Summary")
    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("R²",             f"{ols_model.rsquared:.4f}")
    mc2.metric("Adj. R²",        f"{ols_model.rsquared_adj:.4f}")
    mc3.metric("F-stat p-value", f"{ols_model.f_pvalue:.4f}")
    mc4.metric("Observations",   int(ols_model.nobs))

    # OLS Coefficients table
    st.subheader("OLS Coefficients")
    ols_rows = []
    for feat in FEATURES:
        ols_rows.append({
            "Feature":     feat,
            "Coefficient": round(float(ols_model.params[feat]), 4),
            "Std Error":   round(float(ols_model.bse[feat]), 4),
            "t-stat":      round(float(ols_model.tvalues[feat]), 3),
            "p-value":     round(float(ols_model.pvalues[feat]), 4),
            "Significant": "✅ Yes" if ols_model.pvalues[feat] < 0.05 else "❌ No",
        })
    st.dataframe(pd.DataFrame(ols_rows), hide_index=True, use_container_width=True)

    # VIF table
    st.subheader("VIF — Multicollinearity Check")
    reg_X_const = sm.add_constant(reg_X)
    vif_rows = []
    for i, feat in enumerate(reg_X.columns):
        vif_val = variance_inflation_factor(reg_X_const.values, i + 1)
        vif_rows.append({
            "Feature": feat,
            "VIF":     round(float(vif_val), 2),
            "Flag":    ("🔴 HIGH (>10)" if vif_val > 10
                        else ("🟡 MODERATE (5–10)" if vif_val > 5 else "✅ OK (<5)")),
        })
    vif_df = pd.DataFrame(vif_rows)
    vif_triggered = float(vif_df["VIF"].max()) > 5

    if vif_triggered:
        st.warning("⚠️ Multicollinearity detected (VIF > 5). Ridge and Lasso results shown below.")
    else:
        st.success("✅ No multicollinearity detected (all VIF < 5). OLS coefficients are reliable.")
    st.dataframe(vif_df, hide_index=True, use_container_width=True)

    # Ridge / Lasso (only if VIF triggered)
    if vif_triggered:
        with contextlib.redirect_stdout(io.StringIO()):
            reg_coef_df, _ridge, _lasso, _scaler = run_ridge_lasso(reg_X, reg_y)
        st.subheader("Ridge vs Lasso — Standardized Coefficients")
        st.caption("Coefficients per 1 SD change in each feature. 'YES' in Lasso_zeroed = potentially redundant.")
        st.dataframe(reg_coef_df, hide_index=True, use_container_width=True)

    # Actual vs Predicted scatter
    st.subheader("Actual vs Predicted — Runs per Game")
    pred_vals  = ols_model.predict(reg_X_const)
    scatter_df = reg_dataset[["team", "season"]].copy()
    scatter_df["Actual R/G"]    = reg_y.values
    scatter_df["Predicted R/G"] = pred_vals.values.round(3)
    scatter_df["Residual"]      = (scatter_df["Actual R/G"] - scatter_df["Predicted R/G"]).round(3)

    lo = float(min(scatter_df["Actual R/G"].min(), scatter_df["Predicted R/G"].min())) - 0.15
    hi = float(max(scatter_df["Actual R/G"].max(), scatter_df["Predicted R/G"].max())) + 0.15

    fig_reg = go.Figure()
    fig_reg.add_trace(go.Scatter(
        x=scatter_df["Actual R/G"],
        y=scatter_df["Predicted R/G"],
        mode="markers",
        text=scatter_df["team"] + " " + scatter_df["season"].astype(str),
        hovertemplate="%{text}<br>Actual: %{x:.2f}<br>Predicted: %{y:.2f}<extra></extra>",
        marker=dict(size=8, color="#1f77b4", opacity=0.75),
        name="Team-season",
    ))
    fig_reg.add_trace(go.Scatter(
        x=[lo, hi], y=[lo, hi],
        mode="lines", name="Perfect fit",
        line=dict(dash="dash", color="rgba(180,180,180,0.5)", width=1),
    ))
    fig_reg.update_layout(
        xaxis_title="Actual R/G",
        yaxis_title="Predicted R/G",
        height=480,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right"),
    )
    fig_reg.update_xaxes(gridcolor="rgba(128,128,128,0.15)")
    fig_reg.update_yaxes(gridcolor="rgba(128,128,128,0.15)")
    st.plotly_chart(fig_reg, use_container_width=True,
                    config={"displayModeBar": False}, key="reg_scatter")

    # Full predictions table
    st.subheader("Team-Season Predictions")
    st.dataframe(
        scatter_df.sort_values(["season", "team"]).reset_index(drop=True),
        hide_index=True,
        use_container_width=True,
    )

    st.divider()
    st.caption(
        "Regression data: FanGraphs (via pybaseball) · Baseball Reference · MLB Stats API · "
        "Seasons: " + ", ".join(str(s) for s in sorted(reg_seasons))
    )
    st.stop()


# ── Splits view ───────────────────────────────────────────────────────────────
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

# ── Stale starter banner ──────────────────────────────────────────────────────
# Poll the live MLB API (cached 5 min) and warn if any pitcher changed since
# the last GitHub Actions rebuild.  The banner disappears automatically once
# the next Actions run commits fresh data and Streamlit picks up the new mtime.

live_starters = check_live_starters(date_str)
if live_starters:
    stale_notes = []
    for _, _row in summary.iterrows():
        _gid       = str(_row["game_id"])
        _live      = live_starters.get(_gid)
        if _live is None:
            continue
        _live_home, _live_away = _live
        _saved_home = str(_row.get("home_pitcher_name", "TBD"))
        _saved_away = str(_row.get("away_pitcher_name", "TBD"))
        _changes = []
        if _live_home not in ("TBD", "") and _live_home != _saved_home:
            _changes.append(f"Home: {_saved_home} → **{_live_home}**")
        if _live_away not in ("TBD", "") and _live_away != _saved_away:
            _changes.append(f"Away: {_saved_away} → **{_live_away}**")
        if _changes:
            _matchup = _row.get("matchup", _gid)
            stale_notes.append(f"**{_matchup}** — " + ", ".join(_changes))

    if stale_notes:
        st.warning(
            "⚠️ **Starter update detected** — the dashboard is refreshing automatically "
            "and will update within ~5 minutes.\n\n" + "\n\n".join(stale_notes)
        )

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

            # Load splits so the banner uses the same signals as the per-pitcher badges
            away_splits_df = load_splits(game_id, "away", current_mtime, root=data_root)
            home_splits_df = load_splits(game_id, "home", current_mtime, root=data_root)

            away_team_off = {
                "woba":     game.get("away_woba"),     "wrc_plus": game.get("away_wrc_plus"),
                "obp":      game.get("away_obp"),      "ops_plus": game.get("away_ops_plus"),
                "barrel":   game.get("away_barrel"),   "hard_hit": game.get("away_hard_hit"),
                "k_pct":    game.get("away_k_pct"),    "bb_pct":   game.get("away_bb_pct"),
                "babip":    game.get("away_babip"),
            }
            home_team_off = {
                "woba":     game.get("home_woba"),     "wrc_plus": game.get("home_wrc_plus"),
                "obp":      game.get("home_obp"),      "ops_plus": game.get("home_ops_plus"),
                "barrel":   game.get("home_barrel"),   "hard_hit": game.get("home_hard_hit"),
                "k_pct":    game.get("home_k_pct"),    "bb_pct":   game.get("home_bb_pct"),
                "babip":    game.get("home_babip"),
            }
            away_pred, away_conf, away_color, _, away_sw = predict_runs(
                away_xwoba, away_fip, away_splits_df,
                total_abs=away_total_abs, n_hitters=away_n,
                team_off=away_team_off)
            home_pred, home_conf, home_color, _, home_sw = predict_runs(
                home_xwoba, home_fip, home_splits_df,
                total_abs=home_total_abs, n_hitters=home_n,
                team_off=home_team_off)

            if away_pred is not None and home_pred is not None:
                total      = round(away_pred + home_pred, 1)
                away_team  = game.get("away_team", "Away")
                home_team  = game.get("home_team", "Home")

                # Overall game confidence = lower of the two sides
                conf_rank  = {
                    "High confidence": 3, "Medium confidence": 2,
                    "Low confidence": 1, "Very low — small sample": 0
                }
                away_rank  = conf_rank.get(away_conf, 0)
                home_rank  = conf_rank.get(home_conf, 0)
                if min(away_rank, home_rank) == 3:
                    game_conf  = "High confidence"
                    game_color = "#2ca02c"
                elif min(away_rank, home_rank) == 2:
                    game_conf  = "Medium confidence"
                    game_color = "#ff7f0e"
                elif min(away_rank, home_rank) == 1:
                    game_conf  = "Low confidence"
                    game_color = "#d62728"
                else:
                    game_conf  = "Very low — small sample"
                    game_color = "#9467bd"

                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(135deg, #0d1b2a 0%, #1b263b 100%);
                        border: 1px solid {game_color};
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
                            {away_team}
                            <span style="color:{away_color};">~{away_pred}</span>
                            &nbsp;·&nbsp;
                            {home_team}
                            <span style="color:{home_color};">~{home_pred}</span>
                        </div>
                        <div style="margin-left:auto;text-align:right;">
                            <div style="
                                font-size:0.7rem;
                                font-weight:600;
                                color:{game_color};
                                border:1px solid {game_color};
                                border-radius:20px;
                                padding:2px 8px;
                                display:inline-block;
                                margin-bottom:4px;
                            ">
                                {game_conf}
                            </div>
                            <div style="font-size:0.65rem;color:#778da9;margin-top:2px;">
                                {away_team}: {away_conf} · {home_team}: {home_conf}
                            </div>
                            <div style="font-size:0.65rem;color:#556;">
                                Model: xwOBA + FIP + HardHit% + Whiff%
                            </div>
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
                st.caption(f"Career Statcast splits — {full_team_name} hitters vs {pitcher}")

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
                    # Compute sample weight here so the reliability card
                    # is available before the quadrant tile renders below
                    _sw_card = sample_size_weight(total_abs, int(n) if n else None)
                    _sw_pct  = int(round(_sw_card * 100))
                    if _sw_card >= 0.95:
                        _rel_label = f"{_sw_pct}%"
                        _rel_tier  = "Very Strong"
                        _rel_color = "#1b7e24"
                    elif _sw_card >= 0.80:
                        _rel_label = f"{_sw_pct}%"
                        _rel_tier  = "Strong"
                        _rel_color = "#43a047"
                    elif _sw_card >= 0.65:
                        _rel_label = f"{_sw_pct}%"
                        _rel_tier  = "Moderate"
                        _rel_color = "#ff7f0e"
                    elif _sw_card >= 0.50:
                        _rel_label = f"{_sw_pct}%"
                        _rel_tier  = "Weak"
                        _rel_color = "#f9a825"
                    else:
                        _rel_label = f"{_sw_pct}%"
                        _rel_tier  = "Very Weak"
                        _rel_color = "#9467bd"

                    mc1, mc2, mc3, mc4 = st.columns(4)
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
                        help="Fielding Independent Pitching vs today's opposing lineup (career). "
                             "Lower is better for the pitcher. Scale: <3.20 elite, 3.20–3.79 good, "
                             "3.80–4.19 average, 4.20–4.79 below avg, 5.00+ poor."
                    )
                    with mc4:
                        st.markdown(
                            f"""<div style="padding: 4px 0;">
                                <div style="font-size:0.8rem;color:#888;margin-bottom:4px;">
                                    Quadrant reliability
                                    <span title="How much career AB history exists between this pitcher and the opposing lineup. Very Strong (≥95%) = excellent data. Strong (80–94%) = well-supported. Moderate (65–79%) = reasonably reliable. Weak (50–64%) = treat as a lean. Very Weak (&lt;50%) = insufficient history, quadrant may mislead."
                                          style="cursor:help;"> ⓘ</span>
                                </div>
                                <div style="font-size:1.9rem;font-weight:700;color:{_rel_color};line-height:1.1;">
                                    {_rel_label}
                                </div>
                                <div style="font-size:0.75rem;color:{_rel_color};opacity:0.8;margin-top:2px;">
                                    {_rel_tier}
                                </div>
                            </div>""",
                            unsafe_allow_html=True,
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
                    # Team offense keys — batting team's season stats
                    _team_side = panel["splits_side"]  # "away" batters or "home" batters
                    _team_off  = {
                        "woba":     game.get(f"{_team_side}_woba"),
                        "wrc_plus": game.get(f"{_team_side}_wrc_plus"),
                        "obp":      game.get(f"{_team_side}_obp"),
                        "ops_plus": game.get(f"{_team_side}_ops_plus"),
                        "barrel":   game.get(f"{_team_side}_barrel"),
                        "hard_hit": game.get(f"{_team_side}_hard_hit"),
                        "k_pct":    game.get(f"{_team_side}_k_pct"),
                        "bb_pct":   game.get(f"{_team_side}_bb_pct"),
                        "babip":    game.get(f"{_team_side}_babip"),
                    }
                    pred_runs, conf_label, conf_color, inputs_used, sw = predict_runs(
                        avg_xwoba, fip_val, _splits_preview,
                        total_abs=total_abs, n_hitters=int(n) if n else None,
                        team_off=_team_off
                    )
                    if pred_runs is not None:
                        run_prediction_badge(
                            pred_runs, conf_label, conf_color, batting, inputs_used,
                            sample_weight=sw, total_abs=total_abs,
                            n_hitters=int(n) if n else None
                        )

                # ── FIP vs xwOBA quadrant tile ───────────────────────────
                if show_quadrant:
                    _pitching_abbr = game.get(
                        "home_team" if panel["pitcher_side"] == "home" else "away_team", ""
                    )
                    _pitching_team = TEAM_NAMES.get(_pitching_abbr, _pitching_abbr)
                    quad_result = fip_xwoba_quadrant(
                        avg_xwoba, fip_val, pitcher, full_team_name, _pitching_team,
                        total_abs=total_abs, n_hitters=int(n) if n else None,
                    )
                    if quad_result is not None:
                        q_fig, q_label, q_detail, q_color, q_emoji, q_sw = quad_result
                        q_sw_pct = int(round(q_sw * 100))
                        st.markdown(
                            f"""<div style="
                                background: rgba(255,255,255,0.04);
                                border-left: 4px solid {q_color};
                                border-radius: 8px;
                                padding: 10px 14px;
                                margin: 10px 0 4px 0;
                            ">
                                <span style="font-size:1.0em;font-weight:700;color:{q_color};">
                                    {q_emoji} {q_label}
                                </span><br>
                                <span style="font-size:0.82em;color:#bbb;">{q_detail}</span>
                            </div>""",
                            unsafe_allow_html=True,
                        )
                        st.plotly_chart(
                            q_fig,
                            use_container_width=True,
                            config={"displayModeBar": False},
                            key=f"quadrant_{game_id}_{panel['splits_side']}",
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
