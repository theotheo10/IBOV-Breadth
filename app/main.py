"""
IBOV Breadth API
Endpoints:
  GET /                    → redirect to dashboard
  GET /api/breadth         → full historical breadth series
  GET /api/breadth/latest  → latest snapshot + regime
  GET /api/breadth/range   → date-filtered series
  GET /api/health          → system status
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.engine import (
    load_or_compute_breadth,
    classify_regime,
    BREADTH_PATH,
    PRICES_PATH,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="IBOV Breadth API",
    description="Survivorship-bias-free IBOV market breadth (MA20/50/200)",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve dashboard static files
dashboard_path = Path("dashboard")
if dashboard_path.exists():
    app.mount("/static", StaticFiles(directory="dashboard"), name="static")


# ── Helpers ──────────────────────────────────────────────────────────────────

def breadth_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert breadth DataFrame to clean JSON-serializable records."""
    records = []
    for idx, row in df.iterrows():
        r = {"date": idx.strftime("%Y-%m-%d")}
        for col in ["breadth_20", "breadth_50", "breadth_200",
                    "count_20", "count_50", "count_200", "n_constituents"]:
            val = row.get(col)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                r[col] = None
            else:
                r[col] = round(float(val), 4) if "breadth" in col else int(val)
        records.append(r)
    return records


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    """Serve the dashboard."""
    index = Path("dashboard/index.html")
    if index.exists():
        return FileResponse(str(index))
    return {"message": "IBOV Breadth API — visit /docs for endpoints"}


@app.get("/api/health")
def health():
    """System status check."""
    breadth_exists = BREADTH_PATH.exists()
    prices_exists  = PRICES_PATH.exists()

    last_date = None
    n_rows    = 0
    if breadth_exists:
        try:
            df = pd.read_parquet(BREADTH_PATH)
            last_date = df.index.max().strftime("%Y-%m-%d")
            n_rows    = len(df)
        except Exception:
            pass

    return {
        "status": "ok" if breadth_exists else "needs_init",
        "breadth_data": breadth_exists,
        "prices_data": prices_exists,
        "last_date": last_date,
        "total_rows": n_rows,
    }


@app.get("/api/breadth")
def get_breadth_full(
    ma: Optional[str] = Query(None, description="Filter: '20', '50', '200', or comma-separated e.g. '50,200'"),
):
    """
    Full historical breadth series (2014–present).
    Optional ?ma=200 or ?ma=20,50,200 to filter columns.
    """
    try:
        df = load_or_compute_breadth()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine error: {str(e)}")

    records = breadth_to_records(df)

    # Filter to requested MA windows if specified
    if ma:
        windows = [w.strip() for w in ma.split(",")]
        valid   = {"20", "50", "200"}
        windows = [w for w in windows if w in valid]
        if windows:
            keep_cols = {f"breadth_{w}" for w in windows} | {"date"}
            records   = [{k: v for k, v in r.items() if k in keep_cols} for r in records]

    return {"data": records, "count": len(records)}


@app.get("/api/breadth/latest")
def get_breadth_latest():
    """
    Latest breadth snapshot with regime classification.
    Updates intraday (reflects most recent data available).
    """
    try:
        df = load_or_compute_breadth()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine error: {str(e)}")

    # Get last row with valid breadth_200
    valid = df[df["breadth_200"].notna()]
    if valid.empty:
        raise HTTPException(status_code=404, detail="No valid breadth data found")

    last = valid.iloc[-1]
    b200 = last.get("breadth_200")
    b50  = last.get("breadth_50")
    b20  = last.get("breadth_20")

    return {
        "date":     valid.index[-1].strftime("%Y-%m-%d"),
        "breadth_20":  round(float(b20),  4) if b20  is not None and not np.isnan(b20)  else None,
        "breadth_50":  round(float(b50),  4) if b50  is not None and not np.isnan(b50)  else None,
        "breadth_200": round(float(b200), 4) if b200 is not None and not np.isnan(b200) else None,
        "regime":   classify_regime(b200),
        "n_constituents": int(last.get("n_constituents", 0)),
        "composite": round(
            0.2 * (float(b20 or 0)) +
            0.3 * (float(b50 or 0)) +
            0.5 * (float(b200 or 0)),
            4
        ),
    }


@app.get("/api/breadth/range")
def get_breadth_range(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end:   Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """
    Breadth series filtered by date range.
    E.g. /api/breadth/range?start=2020-01-01&end=2021-12-31
    """
    try:
        df = load_or_compute_breadth()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine error: {str(e)}")

    if start:
        try:
            df = df[df.index >= pd.Timestamp(start)]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid start date format")

    if end:
        try:
            df = df[df.index <= pd.Timestamp(end)]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid end date format")

    if df.empty:
        return {"data": [], "count": 0}

    return {"data": breadth_to_records(df), "count": len(df)}


@app.get("/api/breadth/regime-history")
def get_regime_history():
    """
    Returns breadth_200 with regime classification for every date.
    Useful for backtesting and regime-based allocation.
    """
    try:
        df = load_or_compute_breadth()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine error: {str(e)}")

    valid = df[df["breadth_200"].notna()][["breadth_200", "n_constituents"]]
    records = []
    for idx, row in valid.iterrows():
        b200 = float(row["breadth_200"])
        records.append({
            "date":        idx.strftime("%Y-%m-%d"),
            "breadth_200": round(b200, 4),
            "regime":      classify_regime(b200),
            "n_constituents": int(row.get("n_constituents", 0)),
        })

    return {"data": records, "count": len(records)}
