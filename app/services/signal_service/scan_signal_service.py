
import logging
from app.models.trading_config_model import TradingConfigModel
import pandas as pd
import pandas_ta as ta
import numpy as np


class ScanSignalService:
    def __init__(self):
        pass

    async def scan(self, symbol: str, kline_list: list[any], config: TradingConfigModel):
        logging.info("🔍 Scanning signal for %s kline list count: %s", symbol,
                     len(kline_list))

        if not kline_list or len(kline_list) < 50:
            logging.warning(
                "⚠️ Not enough klines for %s: have %d, need at least 50",
                symbol, len(kline_list) if kline_list else 0
            )
            return {"sig": {"time": None, "type": None}}

        # Extract close prices as Series for EMA calculation
        close_prices = pd.Series([k["close"] for k in kline_list])

        # Calculate EMA 7, 25, 50
        ema_fast = ta.ema(close_prices, length=config.SIGNAL_FAST_EMA_PERIOD)
        ema_slow = ta.ema(close_prices, length=config.SIGNAL_SLOW_EMA_PERIOD)
        ema_50 = ta.ema(close_prices, length=50)

        # Get the last closed candle's open_time
        last_closed_candle = None
        for k in reversed(kline_list):
            if k.get("closed", True):
                last_closed_candle = k
                break

        if not last_closed_candle:
            logging.warning("⚠️ No closed candle found for %s", symbol)
            return {"sig": {"time": None, "type": None}}

        signal_time = last_closed_candle["open_time"]

        # Determine how many candles to check (use SIGNAL_CANDLES or available data)
        check_candles = min(config.SIGNAL_CANDLES, len(kline_list))

        # Get the last N candles for checking alignment
        # Check if EMA alignment is maintained in the recent period
        ema_fast_values = ema_fast.iloc[-check_candles:].values
        ema_slow_values = ema_slow.iloc[-check_candles:].values
        ema_50_values = ema_50.iloc[-check_candles:].values

        # Remove NaN values (from initial periods where EMA is not yet calculated)
        valid_indices = ~(np.isnan(ema_fast_values) | np.isnan(
            ema_slow_values) | np.isnan(ema_50_values))

        if not np.any(valid_indices):
            logging.warning("⚠️ No valid EMA values for %s", symbol)
            return {"sig": {"time": signal_time, "type": None}}

        ema_fast_valid = ema_fast_values[valid_indices]
        ema_slow_valid = ema_slow_values[valid_indices]
        ema_50_valid = ema_50_values[valid_indices]

        # Check for long signal: EMA 7 > 25 > 50 (all aligned in recent period)
        # Check if all recent candles maintain the alignment
        long_aligned = np.all(ema_fast_valid > ema_slow_valid) and np.all(
            ema_slow_valid > ema_50_valid)

        # Check for short signal: EMA 7 < 25 < 50 (all aligned in recent period)
        short_aligned = np.all(ema_fast_valid < ema_slow_valid) and np.all(
            ema_slow_valid < ema_50_valid)

        signal_type = None
        if long_aligned:
            signal_type = "long"
            logging.info("✅ LONG signal detected for %s: EMA %d > %d > 50 aligned",
                         symbol, config.SIGNAL_FAST_EMA_PERIOD, config.SIGNAL_SLOW_EMA_PERIOD)
        elif short_aligned:
            signal_type = "short"
            logging.info("✅ SHORT signal detected for %s: EMA %d < %d < 50 aligned",
                         symbol, config.SIGNAL_FAST_EMA_PERIOD, config.SIGNAL_SLOW_EMA_PERIOD)

        if signal_type is None:
            # logging.warning("⚠️ No signal detected for %s", symbol)
            return None
        else:
            logging.info("✅ Signal detected for %s: %s", symbol, signal_type)
            return {
                "sig": {
                    "time": signal_time,
                    "type": signal_type,
                    "ema_fast": float(ema_fast.iloc[-1]) if not pd.isna(ema_fast.iloc[-1]) else None,
                    "ema_slow": float(ema_slow.iloc[-1]) if not pd.isna(ema_slow.iloc[-1]) else None,
                    "ema_50": float(ema_50.iloc[-1]) if not pd.isna(ema_50.iloc[-1]) else None,
                }
            }
