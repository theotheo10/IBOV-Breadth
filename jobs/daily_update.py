"""
Daily update job — chamado pelo GitHub Actions a cada 15 min durante o pregão.
Na primeira execução: busca histórico completo (~10 min).
Nas seguintes: update incremental (~1 min) + manutenção automática.

ORDEM CRÍTICA:
  1. Verificar rebalanceamento do IBOV (atualiza ibov_composition.py)
  2. Fetchar preços — agora com o universo atualizado
  3. Computar breadth
  4. Manutenção: detectar tickers mortos, backfill de novos tickers
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pathlib import Path

import yfinance as yf
import pandas as pd

from app.engine import (
    load_or_fetch_prices,
    load_or_compute_breadth,
    incremental_update,
    backfill_missing_tickers,
    BREADTH_PATH,
    PRICES_PATH,
)
from data.auto_maintenance import run_maintenance, check_ibov_rebalance

IBOV_PRICE_PATH = Path("data") / "ibov_price.parquet"


def update_ibov_price():
    """Fetch ^BVSP close prices and update ibov_price.parquet."""
    try:
        start = "2014-01-01"
        if IBOV_PRICE_PATH.exists():
            existing = pd.read_parquet(IBOV_PRICE_PATH)
            last = existing.index.max()
            # Only fetch what's missing
            start = (last - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        else:
            existing = None

        from datetime import datetime, timedelta
        end = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

        logger.info(f"Fetching ^BVSP from {start} to {end}...")
        raw = yf.download("^BVSP", start=start, end=end,
                          auto_adjust=True, progress=False, threads=False)

        if raw.empty:
            logger.warning("^BVSP: yfinance returned empty — keeping existing cache")
            return

        close = raw["Close"].squeeze()
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close.name = "close"
        close.index = pd.to_datetime(close.index).normalize()
        close.index.name = "date"
        new_df = close.dropna().to_frame()

        if existing is not None:
            combined = pd.concat([existing, new_df])
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        else:
            combined = new_df

        combined.to_parquet(IBOV_PRICE_PATH)
        logger.info(f"^BVSP saved — {len(combined)} rows, up to {combined.index.max().date()}")

    except Exception as e:
        logger.warning(f"^BVSP fetch failed (non-critical): {e}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    first_run = not BREADTH_PATH.exists() or not PRICES_PATH.exists()

    # ── PASSO 1: rebalanceamento ANTES do fetch ──────────────────────────────
    # Garantimos que ibov_composition.py está atualizado ANTES de qualquer
    # fetch de preços, para que novos tickers entrem no universo imediatamente.
    logger.info("Verificando composição do IBOV...")
    rebalanced = check_ibov_rebalance()
    if rebalanced:
        logger.info("Rebalanceamento aplicado — universo atualizado antes do fetch.")

    # ── PASSO 2: fetch / update de preços ────────────────────────────────────
    if first_run:
        logger.info("═══ PRIMEIRA EXECUÇÃO: construindo histórico completo ═══")
        logger.info("Isso vai levar 5–15 minutos. Aguarde.")

        prices  = load_or_fetch_prices(force_refresh=False)
        breadth = load_or_compute_breadth(force_refresh=False)

        logger.info(f"✓ Preços: {prices.shape}")
        logger.info(f"✓ Breadth: {breadth.shape}")
        logger.info(f"✓ Período: {breadth.index.min().date()} → {breadth.index.max().date()}")

        b200_series = breadth["breadth_200"].dropna()
        if b200_series.empty:
            logger.warning("⚠ breadth_200 sem dados válidos ainda (normal se histórico curto)")
        else:
            b200_last = b200_series.iloc[-1]
            logger.info(f"✓ Último breadth_200: {b200_last:.2%}")
            if not (0.0 <= b200_last <= 1.0):
                logger.error("❌ breadth_200 fora do intervalo — verifique os dados!")
                sys.exit(1)

    else:
        logger.info("═══ UPDATE INCREMENTAL ═══")
        breadth = incremental_update()
        logger.info(f"✓ Atualizado até {breadth.index.max().date()}")

    # ── PASSO 3: backfill de tickers novos na composição ─────────────────────
    # Se o rebalanceamento adicionou tickers que não têm histórico suficiente,
    # busca 250 dias retroativos para que a MA200 possa ser computada.
    prices = load_or_fetch_prices()
    n_backfilled = backfill_missing_tickers(prices)
    if n_backfilled > 0:
        logger.info(f"✓ Backfill: {n_backfilled} tickers com histórico retroativo adicionado")
        # Recomputa breadth após o backfill
        breadth = load_or_compute_breadth(force_refresh=True)
        logger.info(f"✓ Breadth recomputado após backfill: {breadth.shape}")

    # ── PASSO 4: manutenção (detecta mortos, atualiza mapa) ──────────────────
    logger.info("═══ MANUTENÇÃO AUTOMÁTICA ═══")
    prices = load_or_fetch_prices()
    run_maintenance(prices)

    # ── PASSO 5: atualizar preço do IBOV (^BVSP) ────────────────────────────
    logger.info("═══ ATUALIZANDO ^BVSP ═══")
    update_ibov_price()

    logger.info("Concluído.")


if __name__ == "__main__":
    main()
