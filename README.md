# MLB Daily Starter Stats

Fetches today's probable starters and displays their season-to-date Statcast
splits: xwOBA allowed, barrel rate, whiff %, CSW %, exit velocity, and pitch mix.

## Project structure

```
mlb-starter-stats/
├── fetch_daily_stats.py        # Data pipeline (MLB API + pybaseball)
├── app.py                      # Streamlit dashboard
├── requirements.txt
├── data/
│   ├── daily_starters.csv      # Latest (overwritten daily)
│   └── starters_YYYY-MM-DD.csv # Dated snapshots
└── .github/workflows/
    └── daily_stats.yml         # GitHub Actions cron job
```

## Quickstart (local)

```bash
git clone https://github.com/YOUR_USERNAME/mlb-starter-stats.git
cd mlb-starter-stats
pip install -r requirements.txt

python fetch_daily_stats.py   # fetch today's data
streamlit run app.py          # launch dashboard
```

## Deploy free on Streamlit Community Cloud

1. Push this repo to GitHub
2. Go to https://share.streamlit.io → New app → point at app.py
3. Done — live at https://YOUR_APP.streamlit.app

The GitHub Action commits fresh CSVs every morning at 9 AM ET.
Streamlit Community Cloud auto-serves the latest commit.

## Manual refresh

GitHub → your repo → Actions → "Daily Starter Stats" → Run workflow

## Metrics

| Metric | Description |
|---|---|
| xwOBA allowed | Expected wOBA on contact quality — lower = better pitcher |
| Barrel rate | % barreled batted balls — lower = better |
| Whiff % | Swings and misses / swings — higher = better |
| CSW % | Called strikes + whiffs / pitches — higher = better |
| Avg EV | Average exit velocity — lower = better |
| Avg velo | Average release speed |

## Config

Update `SEASON_START` in `fetch_daily_stats.py` each new season.
