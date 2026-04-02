# IBOV Breadth API

Survivorship-bias-free IBOV market breadth indicator.
Tracks % of IBOV constituents above MA20, MA50, MA200 — with historically-correct quarterly rebalancing.

## Features

- **10+ years** of historical data (2014–present)
- **Survivorship-bias-free**: uses correct IBOV composition per date
- **Intraday updates** via GitHub Actions (every 15min during pregão)
- **REST API** + **visual dashboard**
- **Regime classification**: Capitulação / Bear / Neutro / Bull / Sobrecomprado

---

## Quick Start (local)

```bash
# 1. Clone
git clone https://github.com/YOUR_USER/ibov-breadth.git
cd ibov-breadth

# 2. Install
pip install -r requirements.txt

# 3. Build data (first run: ~10 min)
python jobs/daily_update.py

# 4. Run API
uvicorn app.main:app --reload

# 5. Open dashboard
open http://localhost:8000
```

---

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard visual |
| `GET /api/breadth` | Full historical series |
| `GET /api/breadth/latest` | Latest snapshot + regime |
| `GET /api/breadth/range?start=2020-01-01&end=2022-12-31` | Date-filtered |
| `GET /api/breadth/regime-history` | Regime classification for all dates |
| `GET /api/health` | System status |

### Example response — `/api/breadth/latest`

```json
{
  "date": "2025-03-28",
  "breadth_20": 0.4912,
  "breadth_50": 0.5263,
  "breadth_200": 0.4035,
  "regime": "neutral",
  "n_constituents": 87,
  "composite": 0.4587
}
```

---

## Deploy on Render (free)

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) → New Web Service
3. Connect your GitHub repo
4. Render auto-detects `render.yaml`
5. Deploy → your API is live at `https://ibov-breadth.onrender.com`

---

## GitHub Actions — Intraday Updates

The workflow in `.github/workflows/update.yml` runs every 15 minutes
during trading hours (Mon–Fri, 09:00–18:30 BRT).

It:
1. Restores cached price/breadth data
2. Runs incremental update
3. Commits updated `.parquet` files back to repo
4. Render auto-deploys from the new commit

**No secrets needed** — uses default `GITHUB_TOKEN`.

---

## Data Sources

- **Prices**: Yahoo Finance via `yfinance` (adjusted close, auto_adjust=True)
- **IBOV Composition**: Historical quarterly portfolios from B3, encoded in `data/ibov_composition.py`
- **Ticker normalization**: `data/ticker_normalization.py` handles renames, mergers, delistings

---

## Regime Classification

| Breadth 200 | Regime |
|---|---|
| ≥ 80% | Sobrecomprado |
| 60–80% | Bull |
| 40–60% | Neutro |
| 20–40% | Bear |
| < 20% | Capitulação |

---

## Project Structure

```
ibov-breadth/
├── app/
│   ├── main.py          # FastAPI endpoints
│   └── engine.py        # Core breadth computation
├── data/
│   ├── ibov_composition.py    # Historical IBOV constituents (2014–2025)
│   ├── ticker_normalization.py # Ticker renames/mergers
│   ├── prices.parquet         # Generated — cached prices
│   └── breadth.parquet        # Generated — computed breadth
├── jobs/
│   └── daily_update.py  # Update job (called by GitHub Actions)
├── dashboard/
│   └── index.html       # Visual dashboard
├── .github/workflows/
│   └── update.yml       # GitHub Actions workflow
├── requirements.txt
└── render.yaml
```

---

## Important Notes

### Data Quality
- Prices are EOD from Yahoo Finance (free tier, good quality for Brazil)
- Forward-fill limited to 5 days (handles holidays/missing data without distortion)
- MA200 requires 200 trading days of history — stocks with less are excluded from that MA's count
- IBOV composition history was compiled from B3 official quarterly portfolios

### Limitations
- Yahoo Finance occasionally has gaps or bad ticks for Brazilian stocks
- Very old tickers (pre-2014) may have incomplete data
- For institutional use, consider replacing yfinance with B3 direct feed or Bloomberg

### Updating IBOV Composition
When B3 rebalances IBOV (every ~4 months), add a new entry to `data/ibov_composition.py`:

```python
{
    "start": "2025-05-01",
    "end":   "2025-08-31",
    "tickers": ["ABEV3.SA", "VALE3.SA", ...],  # full list from B3
},
```
