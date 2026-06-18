"""
predictor.py — MLB Score Predictor Dashboard
Run locally: streamlit run predictor.py
Reads from data/daily_starters.csv and data/hitter_splits/
"""

import json
import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="MLB Score Predictor",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── League average baselines ──────────────────────────────────────────────────
LG = {
    "woba": 0.312, "wrc_plus": 100, "obp": 0.318, "ops_plus": 100,
    "barrel": 0.080, "hard_hit": 0.380, "k_pct": 0.222, "bb_pct": 0.083,
    "babip": 0.296, "fip": 4.00, "xfip": 4.00, "k_pct_p": 0.222,
    "bb_pct_p": 0.083, "whiff": 0.240, "gb_pct": 0.440,
    "hr9": 1.20, "xwoba": 0.312,
}

PARK_NAMES = {
    "AZ":"Chase Field","ATL":"Truist Park","BAL":"Oriole Park",
    "BOS":"Fenway Park","CHC":"Wrigley Field","CWS":"Guaranteed Rate Field",
    "CIN":"Great American Ball Park","CLE":"Progressive Field",
    "COL":"Coors Field","DET":"Comerica Park","HOU":"Minute Maid Park",
    "KC":"Kauffman Stadium","LAA":"Angel Stadium","LAD":"Dodger Stadium",
    "MIA":"loanDepot park","MIL":"American Family Field","MIN":"Target Field",
    "NYM":"Citi Field","NYY":"Yankee Stadium","OAK":"Oakland Coliseum",
    "PHI":"Citizens Bank Park","PIT":"PNC Park","SD":"Petco Park",
    "SF":"Oracle Park","SEA":"T-Mobile Park","STL":"Busch Stadium",
    "TB":"Tropicana Field","TEX":"Globe Life Field","TOR":"Rogers Centre",
    "WSH":"Nationals Park",
}

TEAM_NAMES = {
    "AZ":"Diamondbacks","ATL":"Braves","BAL":"Orioles","BOS":"Red Sox",
    "CHC":"Cubs","CWS":"White Sox","CIN":"Reds","CLE":"Guardians",
    "COL":"Rockies","DET":"Tigers","HOU":"Astros","KC":"Royals",
    "LAA":"Angels","LAD":"Dodgers","MIA":"Marlins","MIL":"Brewers",
    "MIN":"Twins","NYM":"Mets","NYY":"Yankees","OAK":"Athletics",
    "PHI":"Phillies","PIT":"Pirates","SD":"Padres","SF":"Giants",
    "SEA":"Mariners","STL":"Cardinals","TB":"Rays","TEX":"Rangers",
    "TOR":"Blue Jays","WSH":"Nationals",
}


# ── Score model ───────────────────────────────────────────────────────────────

def safe(val, default):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def score_team(off, pit, hvp_df, park_factor, total_abs, n_hitters):
    import math
    BASE = 4.5

    # Offense (40%)
    off_adj = 0
    off_adj += (safe(off.get("woba"),     LG["woba"])     - LG["woba"])     * 12.0
    off_adj += (safe(off.get("wrc_plus"), LG["wrc_plus"]) - LG["wrc_plus"]) / 100 * 1.2
    off_adj += (safe(off.get("obp"),      LG["obp"])      - LG["obp"])      * 8.0
    off_adj += (safe(off.get("barrel"),   LG["barrel"])   - LG["barrel"])   * 10.0
    off_adj += (safe(off.get("hard_hit"), LG["hard_hit"]) - LG["hard_hit"]) * 4.0
    off_adj -= (safe(off.get("k_pct"),    LG["k_pct"])    - LG["k_pct"])    * 6.0
    off_adj += (safe(off.get("bb_pct"),   LG["bb_pct"])   - LG["bb_pct"])   * 5.0
    off_adj += (safe(off.get("babip"),    LG["babip"])    - LG["babip"])    * 4.0
    off_adj *= 0.40

    # Pitching (35%)
    pit_adj = 0
    pit_adj += (safe(pit.get("fip"),      LG["fip"])      - LG["fip"])      * 0.25
    pit_adj += (safe(pit.get("xfip"),     LG["xfip"])     - LG["xfip"])     * 0.15
    pit_adj -= (safe(pit.get("k_pct"),    LG["k_pct_p"])  - LG["k_pct_p"]) * 4.0
    pit_adj += (safe(pit.get("bb_pct"),   LG["bb_pct_p"]) - LG["bb_pct_p"]) * 3.0
    pit_adj -= (safe(pit.get("whiff"),    LG["whiff"])    - LG["whiff"])    * 3.0
    pit_adj -= (safe(pit.get("gb_pct"),   LG["gb_pct"])   - LG["gb_pct"])  * 2.0
    pit_adj += (safe(pit.get("hr9"),      LG["hr9"])      - LG["hr9"])      * 0.8
    pit_adj += (safe(pit.get("xwoba"),    LG["xwoba"])    - LG["xwoba"])    * 8.0
    pit_adj *= 0.35

    # Hitter vs pitcher history (15%) with sample size weight
    hvp_adj = 0.0
    if hvp_df is not None and not hvp_df.empty:
        sw = 0.0
        if total_abs and total_abs > 0:
            sw = min(1.0, math.log1p(total_abs) / math.log1p(60))
            if n_hitters and n_hitters >= 7:
                sw = min(1.0, sw + 0.10)

        xwoba_col = hvp_df["xwoba"].dropna() if "xwoba" in hvp_df.columns else pd.Series([], dtype=float)
        hh_col    = hvp_df["hard_hit_pct"].dropna() if "hard_hit_pct" in hvp_df.columns else pd.Series([], dtype=float)
        wp_col    = hvp_df["whiff_pct"].dropna() if "whiff_pct" in hvp_df.columns else pd.Series([], dtype=float)

        if not xwoba_col.empty:
            hvp_adj += (xwoba_col.mean() - LG["xwoba"]) * 10.0
        if not hh_col.empty:
            hvp_adj += (hh_col.mean() - LG["hard_hit"]) * 4.0
        if not wp_col.empty:
            hvp_adj -= (wp_col.mean() - LG["whiff"]) * 3.0

        hvp_adj *= sw * 0.15
    else:
        hvp_adj = 0.0

    # Park factor (10%)
    pf       = safe(park_factor, 1.0)
    park_adj = (pf - 1.0) * 3.0 * 0.10

    raw     = BASE + off_adj + pit_adj + hvp_adj + park_adj
    clamped = max(1.0, min(12.0, raw))

    signal_strength = abs(off_adj) + abs(pit_adj)
    if signal_strength > 0.8:
        conf, conf_color = "High confidence",   "success"
    elif signal_strength > 0.4:
        conf, conf_color = "Medium confidence", "warning"
    else:
        conf, conf_color = "Low confidence",    "error"

    return {
        "runs":     round(clamped, 1),
        "conf":     conf,
        "color":    conf_color,
        "off_adj":  round(off_adj,  2),
        "pit_adj":  round(pit_adj,  2),
        "hvp_adj":  round(hvp_adj,  2),
        "park_adj": round(park_adj, 2),
    }


# ── Data loading ──────────────────────────────────────────────────────────────

def get_csv_mtime(path):
    try:
        return str(os.path.getmtime(path))
    except OSError:
        return ""


@st.cache_data(show_spinner="Loading game data...")
def load_summary(date_str, _mtime, data_root="data"):
    path = os.path.join(data_root, "daily_starters.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_splits(game_id, side, _mtime, root="data"):
    fname = f"{game_id}_{side}_vs_{'home' if side == 'away' else 'away'}_pitcher.csv"
    path  = os.path.join(root, "hitter_splits", fname)
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def fmt_adj(v):
    return f"+{v:.2f}" if v >= 0 else f"{v:.2f}"


def conf_badge(label, color):
    colors = {"success": ("#EAF3DE","#3B6D11"), "warning": ("#FAEEDA","#854F0B"), "error": ("#FCEBEB","#A32D2D")}
    bg, fg = colors.get(color, ("#F1EFE8","#5F5E5A"))
    return f'<span style="background:{bg};color:{fg};font-size:11px;font-weight:500;padding:2px 10px;border-radius:20px">{label}</span>'


def stat_bar_html(label, val, lo, hi, invert, pct_fmt=False, default=None):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        val = default or lo
    val   = float(val)
    norm  = max(0.0, min(1.0, (val - lo) / (hi - lo)))
    pct_w = (1 - norm) * 100 if invert else norm * 100
    color = "#639922" if pct_w > 60 else "#E24B4A" if pct_w < 40 else "#BA7517"
    disp  = f"{val*100:.1f}%" if pct_fmt else f"{val:.3f}" if val < 10 else f"{val:.1f}"
    return f"""<div style="display:flex;align-items:center;gap:8px;margin-bottom:5px">
      <span style="font-size:12px;color:var(--color-text-secondary);width:72px;flex-shrink:0">{label}</span>
      <div style="flex:1;height:6px;background:var(--color-background-secondary);border-radius:3px;overflow:hidden">
        <div style="width:{pct_w:.1f}%;height:100%;background:{color};border-radius:3px"></div>
      </div>
      <span style="font-size:12px;font-weight:500;color:var(--color-text-primary);width:44px;text-align:right">{disp}</span>
    </div>"""


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("🎯 Score Predictor")
    tomorrow_available = os.path.exists(os.path.join("data","tomorrow","daily_starters.csv"))
    day_options = ["Today","Tomorrow"] if tomorrow_available else ["Today"]
    selected_day = st.radio("Game day", day_options, horizontal=True)
    is_tomorrow  = selected_day == "Tomorrow"
    data_root    = os.path.join("data","tomorrow") if is_tomorrow else "data"
    selected_date = date.today() + timedelta(days=1) if is_tomorrow else date.today()
    st.divider()
    if st.button("🔄 Force refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    ts_path = os.path.join(data_root, "last_updated.txt")
    if os.path.exists(ts_path):
        with open(ts_path) as f:
            st.caption(f"Data last updated: {f.read().strip()}")

# ── Main ──────────────────────────────────────────────────────────────────────

st.title(f"Score Predictor — {selected_date.strftime('%A, %B %d %Y')}")
st.caption("Weighted model: Offense 40% · Pitching 35% · Hitter vs Pitcher 15% · Park factor 10%")

date_str      = selected_date.strftime("%Y-%m-%d")
csv_path      = os.path.join(data_root, "daily_starters.csv")
current_mtime = get_csv_mtime(csv_path)
summary       = load_summary(date_str, current_mtime, data_root=data_root)

if summary.empty:
    st.warning("No game data found. Run the fetch script or wait for the scheduled Action.")
    st.stop()

st.caption(f"{len(summary)} game{'s' if len(summary) != 1 else ''} found")

for _, game in summary.iterrows():
    matchup   = game.get("matchup","Game")
    game_id   = game["game_id"]
    away_abbr = game.get("away_team","")
    home_abbr = game.get("home_team","")
    away_name = TEAM_NAMES.get(away_abbr, away_abbr)
    home_name = TEAM_NAMES.get(home_abbr, home_abbr)

    game_time_raw = game.get("game_time","")
    try:
        gt = datetime.fromisoformat(game_time_raw.replace("Z","+00:00"))
        game_time_str = gt.astimezone().strftime("%I:%M %p %Z")
    except Exception:
        game_time_str = ""

    with st.expander(f"**{matchup}** — {game_time_str}", expanded=True):

        # Build stat dicts from the row
        away_off = {k: game.get(f"away_{k}") for k in ["woba","wrc_plus","obp","ops_plus","barrel","hard_hit","k_pct","bb_pct","babip"]}
        home_off = {k: game.get(f"home_{k}") for k in ["woba","wrc_plus","obp","ops_plus","barrel","hard_hit","k_pct","bb_pct","babip"]}

        # Pitching stats: home pitcher faces away batters, away pitcher faces home batters
        home_pit = {
            "fip":    game.get("home_pitcher_fip_vs_opp"),
            "xfip":   game.get("home_pit_xfip"),
            "k_pct":  game.get("home_pit_k_pct"),
            "bb_pct": game.get("home_pit_bb_pct"),
            "whiff":  game.get("home_pit_whiff"),
            "gb_pct": game.get("home_pit_gb_pct"),
            "hr9":    game.get("home_pit_hr9"),
            "xwoba":  game.get("away_lineup_avg_xwoba"),
        }
        away_pit = {
            "fip":    game.get("away_pitcher_fip_vs_opp"),
            "xfip":   game.get("away_pit_xfip"),
            "k_pct":  game.get("away_pit_k_pct"),
            "bb_pct": game.get("away_pit_bb_pct"),
            "whiff":  game.get("away_pit_whiff"),
            "gb_pct": game.get("away_pit_gb_pct"),
            "hr9":    game.get("away_pit_hr9"),
            "xwoba":  game.get("home_lineup_avg_xwoba"),
        }

        park_factor  = game.get("park_factor", 1.0)
        away_splits  = load_splits(game_id, "away", current_mtime, root=data_root)
        home_splits  = load_splits(game_id, "home", current_mtime, root=data_root)
        away_total_abs = game.get("away_total_abs")
        home_total_abs = game.get("home_total_abs")
        away_n         = game.get("away_hitters_with_history")
        home_n         = game.get("home_hitters_with_history")

        # Score both teams
        away_score = score_team(away_off, home_pit, away_splits, park_factor, away_total_abs, away_n)
        home_score = score_team(home_off, away_pit, home_splits, park_factor, home_total_abs, home_n)
        total_runs  = round(away_score["runs"] + home_score["runs"], 1)

        # ── Top summary banner ────────────────────────────────────────────────
        c1, c2, c3 = st.columns([5, 3, 5])
        with c1:
            st.metric(f"{away_name} predicted runs", away_score["runs"])
            st.markdown(conf_badge(away_score["conf"], away_score["color"]), unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div style='text-align:center;padding-top:8px'><div style='font-size:11px;color:var(--color-text-secondary)'>Total</div><div style='font-size:28px;font-weight:500'>{total_runs}</div></div>", unsafe_allow_html=True)
        with c3:
            st.metric(f"{home_name} predicted runs", home_score["runs"])
            st.markdown(conf_badge(home_score["conf"], home_score["color"]), unsafe_allow_html=True)

        st.markdown("---")

        # ── Breakdown tabs per team ───────────────────────────────────────────
        tab_away, tab_home = st.tabs([f"{away_name} offense", f"{home_name} offense"])

        for tab, team_name, off, pit, splits_df, score, pitcher_name in [
            (tab_away, away_name, away_off, home_pit, away_splits, away_score, game.get("home_pitcher_name","TBD")),
            (tab_home, home_name, home_off, away_pit, home_splits, home_score, game.get("away_pitcher_name","TBD")),
        ]:
            with tab:
                # Breakdown summary
                b1, b2, b3, b4 = st.columns(4)
                for col, label, val in [
                    (b1, "Offense (40%)",     score["off_adj"]),
                    (b2, "Pitching (35%)",    score["pit_adj"]),
                    (b3, "Hvs P (15%)",       score["hvp_adj"]),
                    (b4, "Park (10%)",         score["park_adj"]),
                ]:
                    col.metric(label, fmt_adj(val))

                st.markdown("---")
                sec1, sec2 = st.columns(2)

                with sec1:
                    st.markdown("**Offense stats**")
                    st.markdown(
                        stat_bar_html("wOBA",     off.get("woba"),     0.280, 0.370, False) +
                        stat_bar_html("wRC+",     off.get("wrc_plus"), 70,    140,   False) +
                        stat_bar_html("OBP",      off.get("obp"),      0.280, 0.380, False) +
                        stat_bar_html("Barrel%",  off.get("barrel"),   0.040, 0.140, False, True) +
                        stat_bar_html("Hard-hit%",off.get("hard_hit"), 0.300, 0.480, False, True) +
                        stat_bar_html("K%",       off.get("k_pct"),    0.140, 0.310, True,  True) +
                        stat_bar_html("BB%",      off.get("bb_pct"),   0.050, 0.140, False, True) +
                        stat_bar_html("BABIP",    off.get("babip"),    0.260, 0.340, False),
                        unsafe_allow_html=True
                    )

                with sec2:
                    st.markdown(f"**{pitcher_name} — pitching stats**")
                    st.markdown(
                        stat_bar_html("FIP",    pit.get("fip"),    2.80, 5.50, True) +
                        stat_bar_html("xFIP",   pit.get("xfip"),   2.80, 5.50, True) +
                        stat_bar_html("K%",     pit.get("k_pct"),  0.140,0.320,False, True) +
                        stat_bar_html("BB%",    pit.get("bb_pct"), 0.040,0.140,True,  True) +
                        stat_bar_html("Whiff%", pit.get("whiff"),  0.160,0.340,False, True) +
                        stat_bar_html("GB%",    pit.get("gb_pct"), 0.360,0.560,False, True) +
                        stat_bar_html("HR/9",   pit.get("hr9"),    0.50, 2.00, True) +
                        stat_bar_html("xwOBA",  pit.get("xwoba"),  0.260,0.380,True),
                        unsafe_allow_html=True
                    )

                st.markdown("---")

                # Hitter vs Pitcher history
                hc1, hc2 = st.columns(2)
                with hc1:
                    st.markdown("**Hitter vs pitcher history**")
                    if splits_df is not None and not splits_df.empty:
                        disp_cols = {
                            "batter_name": "Hitter", "abs": "ABs",
                            "batting_avg": "AVG",    "home_runs": "HR",
                            "xwoba": "xwOBA",        "hard_hit_pct": "HardHit%",
                        }
                        avail = [c for c in disp_cols if c in splits_df.columns]
                        out   = splits_df[avail].rename(columns=disp_cols).copy()
                        if "xwOBA" in out.columns:
                            out["xwOBA"] = out["xwOBA"].apply(lambda v: f"{v:.3f}" if pd.notna(v) else "—")
                        if "HardHit%" in out.columns:
                            out["HardHit%"] = out["HardHit%"].apply(lambda v: f"{v:.1%}" if pd.notna(v) else "—")
                        if "AVG" in out.columns:
                            out["AVG"] = out["AVG"].apply(lambda v: f"{v:.3f}" if pd.notna(v) else "—")
                        st.dataframe(out, use_container_width=True, hide_index=True)
                    else:
                        st.caption("No career history data available for this matchup.")

                with hc2:
                    st.markdown("**Park factor**")
                    park_name = PARK_NAMES.get(home_abbr, home_abbr)
                    pf        = safe(park_factor, 1.0)
                    pct_diff  = round((pf - 1.0) * 100, 1)
                    env       = "Hitter-friendly" if pf > 1.03 else "Pitcher-friendly" if pf < 0.97 else "Neutral"
                    st.metric(park_name, f"{pf:.2f}", f"{pct_diff:+.1f}% vs average")
                    st.caption(f"Run environment: **{env}**")
                    st.caption(f"Park adjustment: **{fmt_adj(score['park_adj'])} runs**")

st.divider()
st.caption("Model: Offense 40% (wOBA/wRC+/OBP/Barrel%/HardHit%/K%/BB%/BABIP) · Pitching 35% (FIP/xFIP/K%/BB%/Whiff%/GB%/HR9/xwOBA) · H vs P 15% (career Statcast splits) · Park 10%")
