"""
mlb_regression_analysis.py
──────────────────────────
Predicts team runs scored per game using sabermetric features.

Steps:
  1. Fetch multi-season team batting data via pybaseball
  2. Fetch runs scored per game from Baseball Reference (via requests)
  3. Run OLS regression with full diagnostics
  4. Check multicollinearity via VIF
  5. If VIF > 5, run Ridge + Lasso with cross-validated alpha selection
  6. Print a clean comparison table of all models

Usage:
  pip install pybaseball pandas numpy scikit-learn statsmodels requests
  python mlb_regression_analysis.py

Output:
  - Printed regression summary (OLS)
  - VIF table for all predictors
  - Ridge / Lasso coefficients if multicollinearity is detected
  - Saved CSV: team_seasons_with_predictions.csv
"""

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import requests
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.linear_model import RidgeCV, LassoCV
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score
from pybaseball import batting_stats, cache

cache.enable()

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

# Seasons to include (skip 2020 — 60-game sample distorts everything)
SEASONS = [2018, 2019, 2021, 2022, 2023, 2024]

# FanGraphs → MLB abbreviation mapping (matches your fetch_daily_stats.py)
FG_TO_MLB = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS",
    "CHC": "CHC", "CWS": "CWS", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KCR": "KCR",
    "LAA": "LAA", "LAD": "LAD", "MIA": "MIA", "MIL": "MIL",
    "MIN": "MIN", "NYM": "NYM", "NYY": "NYY", "OAK": "OAK",
    "PHI": "PHI", "PIT": "PIT", "SDP": "SDP", "SFG": "SFG",
    "SEA": "SEA", "STL": "STL", "TBR": "TBR", "TEX": "TEX",
    "TOR": "TOR", "WSN": "WSN",
}

# Predictors to include in regression
# Adjust this list if you want to test dropping or adding features
FEATURES = [
    "woba",       # Weighted on-base average
    "obp",        # On-base percentage
    "barrel_pct", # Barrel rate (Brl%)
    "hard_hit",   # Hard-hit rate (EV >= 95 mph)
    "k_pct",      # Strikeout rate (applied negatively by model)
    "bb_pct",     # Walk rate
    "babip",      # BABIP
]

# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Fetch team batting stats from FanGraphs via pybaseball
# ─────────────────────────────────────────────────────────────────────────────

def fetch_batting(seasons):
    """Pull team batting stats for each season and stack into one DataFrame."""
    frames = []
    for season in seasons:
        print(f"  Fetching FanGraphs batting stats: {season}...")
        try:
            df = batting_stats(season, qual=0, ind=0)
        except Exception as e:
            print(f"    Warning: failed for {season} — {e}")
            continue

        if "Team" not in df.columns:
            print(f"    Warning: no Team column in {season} data, skipping")
            continue

        def to_rate(val, default=0.0):
            try:
                v = float(val or default)
                return round(v / 100 if v > 1 else v, 4)
            except (TypeError, ValueError):
                return default

        rows = []
        for _, r in df.iterrows():
            fg_abbr = str(r.get("Team", "")).strip()
            if not fg_abbr or fg_abbr not in FG_TO_MLB:
                continue
            rows.append({
                "season":      season,
                "team":        FG_TO_MLB[fg_abbr],
                "fg_abbr":     fg_abbr,
                "woba":        float(r.get("wOBA",   0.312) or 0.312),
                "wrc_plus":    float(r.get("wRC+",   100)   or 100),
                "obp":         float(r.get("OBP",    0.318) or 0.318),
                "ops_plus":    float(r.get("OPS+",   100)   or 100),
                "barrel_pct":  to_rate(r.get("Barrel%",  0.080), 0.080),
                "hard_hit":    to_rate(r.get("HardHit%", 0.380), 0.380),
                "k_pct":       to_rate(r.get("K%",       0.222), 0.222),
                "bb_pct":      to_rate(r.get("BB%",      0.083), 0.083),
                "babip":       float(r.get("BABIP",  0.296) or 0.296),
            })
        frames.append(pd.DataFrame(rows))

    if not frames:
        raise RuntimeError("No batting data fetched — check your internet connection.")

    result = pd.concat(frames, ignore_index=True)
    print(f"  Batting data: {len(result)} team-seasons across {len(seasons)} seasons\n")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Fetch runs scored per game from Baseball Reference
# ─────────────────────────────────────────────────────────────────────────────

def fetch_runs_per_game(seasons):
    """
    Pull team runs scored per game from the Baseball Reference teams table.

    Uses the public HTML tables endpoint — no API key required.
    Returns a DataFrame with columns: season, team_br, R, G, R_per_G
    """
    import io
    frames = []
    for season in seasons:
        url = f"https://www.baseball-reference.com/leagues/majors/{season}-standard-batting.shtml"
        print(f"  Fetching Baseball Reference runs data: {season}...")
        try:
            resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            # BR standard batting table includes team rows at the bottom
            tables = pd.read_html(io.StringIO(resp.text))
        except Exception as e:
            print(f"    Warning: BR fetch failed for {season} — {e}")
            print("    Falling back to MLB Stats API for run totals...")
            frames.append(_fetch_runs_mlb_api(season))
            continue

        # Find the team totals table (has 'Tm' column and 'R' column)
        team_table = None
        for t in tables:
            if "Tm" in t.columns and "R" in t.columns and "G" in t.columns:
                team_table = t
                break

        if team_table is None:
            print(f"    Warning: could not find team table for {season}")
            frames.append(_fetch_runs_mlb_api(season))
            continue

        # Filter to actual team rows (not header repetitions or totals)
        team_table = team_table[team_table["Tm"].notna()].copy()
        team_table = team_table[~team_table["Tm"].isin(["Tm", "League Average", ""])]
        team_table["season"] = season
        team_table["R"]      = pd.to_numeric(team_table["R"], errors="coerce")
        team_table["G"]      = pd.to_numeric(team_table["G"], errors="coerce")
        team_table           = team_table.dropna(subset=["R", "G"])
        team_table["R_per_G"] = (team_table["R"] / team_table["G"]).round(3)
        frames.append(team_table[["season", "Tm", "R", "G", "R_per_G"]].rename(
            columns={"Tm": "team_br"}
        ))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_runs_mlb_api(season):
    """
    Fallback: pull runs + games from MLB Stats API standings endpoint.
    Returns DataFrame with columns: season, team_br, R, G, R_per_G
    """
    url    = "https://statsapi.mlb.com/api/v1/standings"
    params = {
        "leagueId":  "103,104",
        "season":    season,
        "standingsTypes": "regularSeason",
        "hydrate":   "team,league",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        records = r.json().get("records", [])
    except Exception as e:
        print(f"    MLB API fallback also failed for {season}: {e}")
        return pd.DataFrame()

    rows = []
    for division in records:
        for entry in division.get("teamRecords", []):
            abbr = entry.get("team", {}).get("abbreviation", "")
            rs   = entry.get("runsScored",   None)
            ra   = entry.get("runsAllowed",  None)
            g    = entry.get("gamesPlayed",  None)
            if abbr and rs is not None and g:
                rows.append({
                    "season":   season,
                    "team_br":  abbr,
                    "R":        int(rs),
                    "G":        int(g),
                    "R_per_G":  round(int(rs) / int(g), 3),
                })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Join batting stats with runs per game
# ─────────────────────────────────────────────────────────────────────────────

# BR team abbreviations differ slightly from FanGraphs in a few cases
BR_TO_FG = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS",
    "CHC": "CHC", "CHW": "CWS", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KCR": "KCR",
    "LAA": "LAA", "LAD": "LAD", "MIA": "MIA", "MIL": "MIL",
    "MIN": "MIN", "NYM": "NYM", "NYY": "NYY", "OAK": "OAK",
    "PHI": "PHI", "PIT": "PIT", "SDP": "SDP", "SFG": "SFG",
    "SEA": "SEA", "STL": "STL", "TBR": "TBR", "TEX": "TEX",
    "TOR": "TOR", "WSN": "WSN",
    # Some seasons use slightly different codes:
    "TB":  "TBR", "KC":  "KCR", "SD":  "SDP",
    "SF":  "SFG", "WSH": "WSN", "AZ":  "ARI",
}


def build_dataset(batting_df, runs_df):
    """Join batting stats with runs/game, reconciling team abbreviations."""
    runs_df = runs_df.copy()
    runs_df["team"] = runs_df["team_br"].map(BR_TO_FG).fillna(runs_df["team_br"])

    merged = pd.merge(
        batting_df, runs_df[["season", "team", "R_per_G"]],
        on=["season", "team"], how="inner"
    )
    if merged.empty:
        print("WARNING: Merge returned 0 rows — check team abbreviation mapping.")
    else:
        print(f"  Dataset ready: {len(merged)} team-seasons, {merged['season'].nunique()} seasons\n")
    return merged.dropna(subset=FEATURES + ["R_per_G"])


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — OLS Regression
# ─────────────────────────────────────────────────────────────────────────────

def run_ols(df):
    """Run OLS regression of R_per_G ~ FEATURES. Returns model and design matrix."""
    X = df[FEATURES].copy()
    y = df["R_per_G"]

    X_const = sm.add_constant(X)
    model   = sm.OLS(y, X_const).fit()

    print("=" * 68)
    print("OLS REGRESSION — Runs per Game ~ Sabermetric Features")
    print("=" * 68)
    print(model.summary())
    return model, X, y


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — VIF Check
# ─────────────────────────────────────────────────────────────────────────────

def check_vif(X):
    """
    Compute Variance Inflation Factor for each predictor.
    VIF > 5  → moderate multicollinearity, worth investigating
    VIF > 10 → serious multicollinearity, drop or regularize
    """
    X_const = sm.add_constant(X)
    vif_data = pd.DataFrame()
    vif_data["Feature"] = X_const.columns
    vif_data["VIF"]     = [
        variance_inflation_factor(X_const.values, i)
        for i in range(X_const.shape[1])
    ]
    # Drop the constant row — VIF is undefined / meaningless for it
    vif_data = vif_data[vif_data["Feature"] != "const"].reset_index(drop=True)
    vif_data["VIF"] = vif_data["VIF"].round(2)
    vif_data["Flag"] = vif_data["VIF"].apply(
        lambda v: "🔴 HIGH (>10)" if v > 10 else ("🟡 MODERATE (5-10)" if v > 5 else "✅ OK (<5)")
    )

    print("\n" + "=" * 68)
    print("VIF — MULTICOLLINEARITY CHECK")
    print("=" * 68)
    print(vif_data.to_string(index=False))
    print()

    high_vif = vif_data[vif_data["VIF"] > 5]
    if high_vif.empty:
        print("✅ No multicollinearity detected (all VIF < 5). OLS coefficients are reliable.\n")
        return False
    else:
        print(f"⚠️  Multicollinearity detected in {len(high_vif)} variable(s).")
        print("   Proceeding with Ridge and Lasso regularization to handle this.\n")
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — Ridge and Lasso Regression
# ─────────────────────────────────────────────────────────────────────────────

def run_ridge_lasso(X, y):
    """
    Fit Ridge and Lasso with cross-validated alpha selection.
    Standardizes features first (required for regularized regression).
    Returns a comparison DataFrame of feature coefficients.
    """
    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Ridge: shrinks all coefficients toward zero, keeps all features
    alphas  = np.logspace(-3, 3, 100)
    ridge   = RidgeCV(alphas=alphas, cv=5).fit(X_scaled, y)
    ridge_cv_r2 = cross_val_score(ridge, X_scaled, y, cv=5, scoring="r2").mean()

    # Lasso: can zero out coefficients entirely (feature selection)
    lasso   = LassoCV(cv=5, max_iter=10000, random_state=42).fit(X_scaled, y)
    lasso_cv_r2 = cross_val_score(lasso, X_scaled, y, cv=5, scoring="r2").mean()

    print("=" * 68)
    print("RIDGE REGRESSION")
    print("=" * 68)
    print(f"  Best alpha (regularization strength): {ridge.alpha_:.4f}")
    print(f"  Cross-validated R²: {ridge_cv_r2:.4f}\n")

    print("=" * 68)
    print("LASSO REGRESSION")
    print("=" * 68)
    print(f"  Best alpha: {lasso.alpha_:.4f}")
    print(f"  Cross-validated R²: {lasso_cv_r2:.4f}")
    zeroed = [f for f, c in zip(FEATURES, lasso.coef_) if abs(c) < 1e-6]
    if zeroed:
        print(f"  Features zeroed out by Lasso: {zeroed}")
        print("  → These features may be redundant given the others.")
    else:
        print("  → Lasso kept all features (no redundancy severe enough to zero out).")
    print()

    # Comparison table: standardized coefficients side by side
    coef_df = pd.DataFrame({
        "Feature":     FEATURES,
        "Ridge_coef":  ridge.coef_.round(4),
        "Lasso_coef":  lasso.coef_.round(4),
        "Lasso_zeroed": ["YES" if abs(c) < 1e-6 else "no" for c in lasso.coef_],
    })

    print("=" * 68)
    print("STANDARDIZED COEFFICIENT COMPARISON (Ridge vs Lasso)")
    print("(Coefficients represent effect per 1 SD change in each feature)")
    print("=" * 68)
    print(coef_df.to_string(index=False))
    print()

    return coef_df, ridge, lasso, scaler


# ─────────────────────────────────────────────────────────────────────────────
# Step 7 — Interpretation helper
# ─────────────────────────────────────────────────────────────────────────────

def print_interpretation(ols_model, vif_triggered, coef_df=None):
    """Print a plain-English summary of findings."""
    print("=" * 68)
    print("INTERPRETATION SUMMARY")
    print("=" * 68)

    r2 = ols_model.rsquared
    print(f"\n  OLS R²: {r2:.4f} — the model explains {r2*100:.1f}% of variance in R/G")

    pvals = ols_model.pvalues.drop("const")
    sig   = pvals[pvals < 0.05]
    insig = pvals[pvals >= 0.05]

    print(f"\n  Statistically significant features (p < 0.05):")
    for f, p in sig.sort_values().items():
        coef = ols_model.params[f]
        print(f"    {f:<14} coef={coef:+.4f}  p={p:.4f}")

    if not insig.empty:
        print(f"\n  Not statistically significant (p >= 0.05):")
        for f, p in insig.sort_values().items():
            coef = ols_model.params[f]
            print(f"    {f:<14} coef={coef:+.4f}  p={p:.4f}")
        print("  → Consider dropping these or reviewing collinearity.")

    if vif_triggered and coef_df is not None:
        zeroed = coef_df[coef_df["Lasso_zeroed"] == "YES"]["Feature"].tolist()
        if zeroed:
            print(f"\n  Lasso zeroed out: {zeroed}")
            print("  → Strong evidence these features are redundant with others in the set.")
            print("  → For your run prediction model, prefer Ridge (shrinks but keeps all)")
            print("    or drop the zeroed features and re-run OLS for cleaner coefficients.")

    print("""
  Recommended next steps:
  1. If VIF > 10 on wOBA + OBP: drop OBP (wOBA subsumes it mathematically).
  2. If VIF > 10 on Barrel% + HardHit%: create a composite contact_quality
     feature = (barrel_pct * 0.6 + hard_hit * 0.4) instead of two separate vars.
  3. Rerun OLS on the trimmed feature set and check that all p-values < 0.05.
  4. Use Ridge coefficients for your run prediction model if you want to keep
     all features — Ridge handles collinearity without eliminating variables.
  5. Validate on held-out seasons (e.g. train on 2018-2022, test on 2023-2024).
""")


# ─────────────────────────────────────────────────────────────────────────────
# Step 8 — Save results
# ─────────────────────────────────────────────────────────────────────────────

def save_results(df, ols_model):
    """Append OLS predictions to the dataset and save to CSV."""
    X_const         = sm.add_constant(df[FEATURES])
    df = df.copy()
    df["predicted_R_per_G"] = ols_model.predict(X_const).round(3)
    df["residual"]          = (df["R_per_G"] - df["predicted_R_per_G"]).round(3)

    out = "team_seasons_with_predictions.csv"
    df.to_csv(out, index=False)
    print(f"  Saved: {out}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "=" * 68)
    print("MLB RUN PREDICTION — REGRESSION ANALYSIS")
    print("=" * 68 + "\n")

    print("Step 1 — Fetching team batting stats (FanGraphs via pybaseball)...")
    batting_df = fetch_batting(SEASONS)

    print("Step 2 — Fetching team runs scored (Baseball Reference)...")
    runs_df = fetch_runs_per_game(SEASONS)

    print("Step 3 — Building dataset...")
    dataset = build_dataset(batting_df, runs_df)

    if dataset.empty:
        print("ERROR: Dataset is empty after joining. Check abbreviation mappings.")
        raise SystemExit(1)

    print("Step 4 — OLS Regression...\n")
    ols_model, X, y = run_ols(dataset)

    print("\nStep 5 — VIF Multicollinearity Check...\n")
    vif_triggered = check_vif(X)

    coef_df = None
    if vif_triggered:
        print("Step 6 — Ridge and Lasso Regularization...\n")
        coef_df, ridge_model, lasso_model, scaler = run_ridge_lasso(X, y)

    print_interpretation(ols_model, vif_triggered, coef_df)

    print("Step 7 — Saving results...")
    save_results(dataset, ols_model)

    print("\nDone.")
