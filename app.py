"""
app.py  —  MLB Hitter Splits vs Today's Starting Pitchers
Run locally:  streamlit run app.py
"""

import os
from datetime import date, datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from fetch_daily_stats import build_daily_report

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
    display_cols = {
        "batter_name":   "Hitter",
        "seasons":       "Seasons",
        "pa":            "PA (career)",
        "pitches_seen":  "Pitches",
        "xwoba":         "xwOBA",
        "barrel_rate":   "Barrel%",
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
        elif raw in ("pa", "pitches_seen"):
            out[label] = out[label].apply(lambda v: f"{int(v)}" if pd.notna(v) else "—")
        elif raw in ("barrel_rate", "hard_hit_pct", "whiff_pct"):
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

@st.cache_data(ttl=1800, show_spinner="Fetching today's starters and hitter splits…")
def load_summary(game_date_str):
    csv_path = os.path.join("data", "daily_starters.csv")
    if os.path.exists(csv_path):
        mtime = datetime.fromtimestamp(os.path.getmtime(csv_path), tz=timezone.utc)
        if mtime.date() == date.today():
            return pd.read_csv(csv_path)
    df = build_daily_report(game_date_str)
    if not df.empty:
        os.makedirs("data", exist_ok=True)
        df.to_csv(csv_path, index=False)
    return df


def load_splits(game_id, side):
    """Load per-game hitter splits CSV if it exists."""
    fname = f"{game_id}_{side}_vs_{'home' if side == 'away' else 'away'}_pitcher.csv"
    path  = os.path.join("data", "hitter_splits", fname)
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("⚾ MLB Starter Splits")
    selected_date = st.date_input("Game date", value=date.today())
    st.caption("Shows each opposing hitter's Statcast stats vs. today's starter — season-to-date.")
    st.divider()
    show_chart = st.toggle("Show xwOBA chart", value=True)
    show_table = st.toggle("Show hitter table", value=True)
    st.divider()
    if st.button("🔄 Force refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Last refreshed: {datetime.now().strftime('%I:%M %p')}")

# ── Main ──────────────────────────────────────────────────────────────────────

st.title(f"Hitter Splits vs Starters — {selected_date.strftime('%A, %B %d %Y')}")
st.caption("Each panel shows the **opposing lineup's** career Statcast numbers vs. that starting pitcher (all seasons since 2015).")

date_str = selected_date.strftime("%Y-%m-%d")
summary  = load_summary(date_str)

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

        col_left, col_div, col_right = st.columns([5, 0.2, 5])

        for col, panel in zip([col_left, col_right], panels):
            with col:
                pitcher = panel["pitcher_name"]
                batting = panel["batting_team"]

                st.subheader(f"{pitcher}")
                st.caption(f"{batting} hitters vs this pitcher")

                if not pitcher or pitcher == "TBD":
                    st.info("Starter not yet announced.")
                    continue

                n = panel["hitters_with_history"]
                avg_xwoba = panel["lineup_avg_xwoba"]

                if n is not None and n > 0:
                    mc1, mc2 = st.columns(2)
                    mc1.metric("Hitters with history", int(n))
                    mc2.metric("Lineup avg xwOBA", f"{avg_xwoba:.3f}" if avg_xwoba else "—")
                else:
                    st.info("No head-to-head Statcast history found for this matchup yet "
                            "(common early in the season or for new pitchers).")
                    continue

                splits_df = load_splits(game_id, panel["splits_side"])

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
