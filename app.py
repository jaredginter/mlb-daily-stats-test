"""
app.py  —  MLB Daily Starter Stats Dashboard
Run locally:  streamlit run app.py
Deploy free:  https://streamlit.io/cloud  (connect your GitHub repo)
"""

import json
import os
from datetime import date, datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from fetch_daily_stats import build_daily_report

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MLB Starter Stats",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded",
)

METRICS = {
    "xwoba_allowed": ("xwOBA allowed", ".3f", "lower"),
    "barrel_rate":   ("Barrel rate",   ".1%", "lower"),
    "whiff_pct":     ("Whiff %",       ".1%", "higher"),
    "csw_pct":       ("CSW %",         ".1%", "higher"),
    "avg_exit_velo": ("Avg EV (mph)",  ".1f", "lower"),
    "avg_velo":      ("Avg velo (mph)", ".1f", None),
    "pitches":       ("Pitches (season)", ",d", None),
}

PITCH_LABELS = {
    "FF": "4-Seam FB", "SI": "Sinker",  "FC": "Cutter",
    "SL": "Slider",    "CH": "Changeup","CU": "Curveball",
    "KC": "Knuckle-curve", "FS": "Splitter",
    "ST": "Sweeper",   "SV": "Slurve",
}


def fmt(value, fmt_str):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        return f"{value:{fmt_str}}"
    except Exception:
        return str(value)


def pitch_mix_chart(mix_json, pitcher_name):
    if not mix_json:
        return None
    try:
        mix = json.loads(mix_json) if isinstance(mix_json, str) else mix_json
    except Exception:
        return None
    labels = [PITCH_LABELS.get(k, k) for k in mix]
    values = list(mix.values())
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.45,
        textinfo="label+percent", textfont_size=12,
        marker_colors=["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd"],
    ))
    fig.update_layout(
        title=dict(text=f"{pitcher_name} — pitch mix", font_size=13),
        showlegend=False,
        margin=dict(t=36, b=0, l=0, r=0),
        height=220,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


@st.cache_data(ttl=1800, show_spinner="Fetching today's starters…")
def load_data(game_date_str):
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


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚾ MLB Starter Stats")
    selected_date = st.date_input("Game date", value=date.today())
    st.caption("Statcast splits are season-to-date through today.")
    st.divider()
    show_charts = st.toggle("Show pitch mix charts", value=True)
    st.divider()
    if st.button("🔄 Force refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Last refreshed: {datetime.now().strftime('%I:%M %p')}")


# ── Main ──────────────────────────────────────────────────────────────────────
st.title(f"Probable Starters — {selected_date.strftime('%A, %B %d %Y')}")

date_str = selected_date.strftime("%Y-%m-%d")
df = load_data(date_str)

if df.empty:
    st.warning("No games or probable starters found for this date. "
               "Try again later — MLB usually posts starters by mid-morning.")
    st.stop()

st.caption(f"{len(df)} game{'s' if len(df) != 1 else ''} found")

for _, row in df.iterrows():
    matchup = row.get("matchup", "Game")
    game_time_raw = row.get("game_time", "")
    try:
        gt = datetime.fromisoformat(game_time_raw.replace("Z", "+00:00"))
        game_time_str = gt.astimezone().strftime("%I:%M %p %Z")
    except Exception:
        game_time_str = ""

    with st.expander(f"**{matchup}**  {game_time_str}", expanded=True):
        away_col, divider_col, home_col = st.columns([5, 0.3, 5])

        for col, side in [(away_col, "away"), (home_col, "home")]:
            pitcher = row.get(f"{side}_pitcher", "TBD")
            team_label = row.get("away_team" if side == "away" else "home_team", "")
            with col:
                st.subheader(pitcher or "TBD")
                st.caption(f"{'Away' if side == 'away' else 'Home'} — {team_label}")

                if not pitcher or pitcher == "TBD":
                    st.info("Probable starter not yet announced.")
                    continue

                m1, m2, m3 = st.columns(3)
                m4, m5, m6 = st.columns(3)
                shown = ["xwoba_allowed","barrel_rate","whiff_pct",
                         "csw_pct","avg_exit_velo","avg_velo"]
                for mcol, key in zip([m1,m2,m3,m4,m5,m6], shown):
                    label, fmt_str, _ = METRICS[key]
                    val = row.get(f"{side}_{key}")
                    mcol.metric(label, fmt(val, fmt_str))

                pitches = row.get(f"{side}_pitches")
                if pitches:
                    st.caption(f"Season pitches tracked: {int(pitches):,}")

                if show_charts:
                    fig = pitch_mix_chart(row.get(f"{side}_pitch_mix"), pitcher)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True,
                                        key=f"{side}_{row['game_id']}_mix")

        with divider_col:
            st.markdown(
                "<div style='text-align:center;font-size:1.4rem;"
                "padding-top:2rem;color:#888'>vs</div>",
                unsafe_allow_html=True,
            )

st.divider()
st.caption("Data: Baseball Savant (Statcast) via pybaseball · MLB Stats API · "
           "Stats are season-to-date and update each morning via GitHub Actions.")
