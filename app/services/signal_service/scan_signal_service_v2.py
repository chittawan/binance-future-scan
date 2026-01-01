import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as ta

from app.models.trading_config_model import TradingConfigModel


@dataclass
class ScanResult:
    symbol: str
    time: int
    trend: Optional[str]        # LONG / SHORT / None
    # LONG_START, LONG_CONTINUE, SHORT_START, SHORT_CONTINUE, WATCH, NONE
    state: str
    score: int
    ma_fast: Optional[float]
    ma_slow: Optional[float]
    ma_50: Optional[float]
    adx: Optional[float]


class ScanSignalServiceV2:
    """
    Scanner only:
    - Identify trend & strength
    - Rank symbols
    - No entry / SL / TP
    """

    def __init__(self):
        pass

    async def scan(
        self,
        symbol: str,
        kline_list: list[dict],
        config: TradingConfigModel,
    ) -> Optional[ScanResult]:

        if not kline_list or len(kline_list) < 60:
            return None

        # =========================
        # Prepare Data
        # =========================
        df = pd.DataFrame(kline_list)

        # ใช้ candle ปิดเท่านั้น
        if "closed" in df.columns:
            df = df[df["closed"] == True]

        if len(df) < 60:
            return None

        close = df["close"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        volume = df["volume"].astype(float)

        signal_time = int(df.iloc[-1]["open_time"])

        # =========================
        # Indicators
        # =========================
        ma_fast = ta.sma(close, length=config.SIGNAL_FAST_EMA_PERIOD)
        ma_slow = ta.sma(close, length=config.SIGNAL_SLOW_EMA_PERIOD)
        ma_50 = ta.sma(close, length=50)

        adx_df = ta.adx(high, low, close, length=14)
        adx = adx_df["ADX_14"]

        # Last values
        ef, es, e50 = ma_fast.iloc[-1], ma_slow.iloc[-1], ma_50.iloc[-1]
        adx_last = adx.iloc[-1]

        if np.isnan([ef, es, e50, adx_last]).any():
            return None

        # =========================
        # Trend Alignment
        # =========================
        long_aligned = ef > es > e50
        short_aligned = ef < es < e50

        # =========================
        # Momentum (Slope)
        # =========================
        ef_slope = ma_fast.diff().iloc[-1]
        es_slope = ma_slow.diff().iloc[-1]

        long_momentum = ef_slope > 0 and es_slope > 0
        short_momentum = ef_slope < 0 and es_slope < 0

        # =========================
        # ADX Filter (Trend strength)
        # =========================
        trend_ok = adx_last >= 20

        # =========================
        # Detect START vs CONTINUE
        # =========================
        prev_ef = ma_fast.iloc[-2]
        prev_es = ma_slow.iloc[-2]
        prev_e50 = ma_50.iloc[-2]

        prev_long_aligned = prev_ef > prev_es > prev_e50
        prev_short_aligned = prev_ef < prev_es < prev_e50

        # =========================
        # WATCH condition (early trend)
        # =========================
        watch_long = (
            ef > es
            and abs(es - e50) / e50 < 0.01
            and ef_slope > 0
        )

        watch_short = (
            ef < es
            and abs(es - e50) / e50 < 0.01
            and ef_slope < 0
        )

        # =========================
        # Scoring System
        # =========================
        score = 0

        if long_aligned or short_aligned:
            score += 3

        if long_momentum or short_momentum:
            score += 2

        if adx_last >= 25:
            score += 2
        elif adx_last >= 20:
            score += 1

        vol_ma = volume.rolling(20).mean().iloc[-1]
        if volume.iloc[-1] > vol_ma:
            score += 1

        # =========================
        # Final State Decision
        # =========================
        state = "NONE"
        trend = None

        if long_aligned and long_momentum and trend_ok:
            trend = "LONG"
            state = "LONG_START" if not prev_long_aligned else "LONG_CONTINUE"

        elif short_aligned and short_momentum and trend_ok:
            trend = "SHORT"
            state = "SHORT_START" if not prev_short_aligned else "SHORT_CONTINUE"

        elif watch_long or watch_short:
            state = "WATCH"
            score = max(score, 2)

        else:
            return None  # ตัดทิ้งตั้งแต่ scanner layer

        return ScanResult(
            symbol=symbol,
            time=signal_time,
            trend=trend,
            state=state,
            score=score,
            ma_fast=float(ef),
            ma_slow=float(es),
            ma_50=float(e50),
            adx=float(adx_last),
        )
