"""
Candle Service for calculating trading signals based on Heikin-Ashi candles and EMA indicators.

This service implements a TradingView Pine Script strategy that uses:
- Heikin-Ashi candles
- Fast and Slow EMA indicators
- Multiple signal conditions (Green, Blue, LBlue, Red, Orange, Yellow, GreenRed)
- Position management logic
"""

from datetime import datetime
import logging
import pandas as pd
from app.models.trading_config_model import TradingConfigModel
import pandas_ta as ta
import copy

log = logging.getLogger(__name__)


class CandleService:

    async def cal_signal(self, kline: list, config: TradingConfigModel, isDebug: bool = False):
        # Deep copy kline เพื่อป้องกันการ modify ข้อมูลต้นฉบับ
        # โดยเฉพาะเมื่อ websocket อาจจะ update ราคาไปแล้ว
        kline = copy.deepcopy(kline)

        FastEMA_period = config.SIGNAL_FAST_EMA_PERIOD
        SlowEMA_period = config.SIGNAL_SLOW_EMA_PERIOD

        isUseLong = config.USE_LONG
        isUseGreenOnly = config.USE_GREEN_ONLY and isUseLong
        isUseBlue = config.USE_BLUE and isUseLong
        isUseLBlue = config.USE_LBLUE and isUseLong

        isUseShort = config.USE_SHORT
        isUseRed = config.USE_RED and isUseShort
        isUseOrange = config.USE_ORANGE and isUseShort
        isUseYellow = config.USE_YELLOW and isUseShort
        isUseGreenRed = config.USE_GREEN_RED and isUseShort

        # ถ้าแท่งล่าสุดปิดแล้ว ให้เพิ่มแท่งใหม่ที่ยังไม่ปิด
        # เพื่อให้ confirm_idx = 1 ชี้ไปที่แท่งที่ปิดแล้ว (ไม่พลาดสัญญาณ)
        if kline[-1]["closed"]:
            # ตรวจสอบว่ามีอย่างน้อย 2 แท่ง (ต้องใช้ kline[-2] ในการคำนวณ interval)
            if len(kline) < 2:
                log.warning(
                    "⚠️ Not enough klines to create new open candle, need at least 2")
            else:
                last_kline = kline[-1].copy()
                last_kline["closed"] = False
                # คำนวณ open_time ของแท่งใหม่ = open_time ของแท่งล่าสุด + interval
                open_time = kline[-1]["open_time"] + \
                    (kline[-1]["open_time"] - kline[-2]["open_time"])
                # check if future time skip append new candle

                last_kline["open_time"] = open_time
                # ใช้ราคาปิดของแท่งที่ปิดแล้วเป็นราคาเปิดของแท่งใหม่
                close_price = kline[-1]["close"]
                last_kline["open"] = close_price
                # สำหรับแท่งที่ยังไม่ปิด เริ่มต้นที่ราคาเดียวกัน (จะอัพเดทเมื่อมีข้อมูลใหม่)
                last_kline["high"] = close_price
                last_kline["low"] = close_price
                last_kline["close"] = close_price
                last_kline["volume"] = 0
                kline.append(last_kline)

        # Calculate Heikin-Ashi candles
        timestamp, h_o, h_h, h_l, h_c, original_ema_7, original_ema_25, original_ema_50 = self.calc_heikinashi(
            kline)

        # Calculate EMAs on Heikin-Ashi close prices
        xPrice = ta.ema(pd.Series(h_c), 1)
        ema_fast = ta.sma(xPrice, FastEMA_period)
        ema_slow = ta.sma(xPrice, SlowEMA_period)

        # Calculate fixed MAs for trend conditions
        fix_ma_7 = ta.sma(pd.Series(h_c), 7)
        fix_ma_25 = ta.sma(pd.Series(h_c), 25)

        # Convert to pandas Series for vectorized operations
        # Fill NaN values with 0 to prevent comparison errors
        h_o_series = pd.Series(h_o).fillna(0)
        h_c_series = pd.Series(h_c).fillna(0)
        xPrice_series = pd.Series(xPrice).fillna(0)
        ema_fast_series = pd.Series(ema_fast).fillna(0)
        ema_slow_series = pd.Series(ema_slow).fillna(0)

        # Calculate bull and bear conditions (same as v1)
        # Use vectorized comparison with NaN handling
        bull = ema_fast_series > ema_slow_series
        bear = ema_fast_series < ema_slow_series
        bull_series = pd.Series(bull)
        bear_series = pd.Series(bear)

        # Vectorized signal calculations
        # Green = Bull and xPrice > FastMA  // Buy
        green = bull_series & (xPrice_series > ema_fast_series)
        green_only = green & (h_c_series > h_o_series)
        green_red = green & (h_o_series > h_c_series)

        # Blue = Bear and xPrice > FastMA and xPrice > SlowMA  //Pre Buy 2
        blue = bear_series & (xPrice_series > ema_fast_series) & (
            xPrice_series > ema_slow_series)
        # LBlue = Bear and xPrice > FastMA and xPrice < SlowMA  //Pre Buy 1
        lblue = bear_series & (xPrice_series > ema_fast_series) & (
            xPrice_series < ema_slow_series)
        # Red = Bear and xPrice < FastMA  // Sell
        red = bear_series & (xPrice_series < ema_fast_series)
        # Orange = Bull and xPrice < FastMA and xPrice < SlowMA  // Pre Sell 2
        orange = bull_series & (xPrice_series < ema_fast_series) & (
            xPrice_series < ema_slow_series)
        # Yellow = Bull and xPrice < FastMA and xPrice > SlowMA  // Pre Sell 1
        yellow = bull_series & (xPrice_series < ema_fast_series) & (
            xPrice_series > ema_slow_series)

        # Vectorized signal assignment following Pine color priority
        # Initialize with "None"
        signals = pd.Series(["None"] * len(h_c))

        # Apply priority matching TradingView ternary operator and v1 if-elif logic:
        # TradingView: Green ? #3fff00 : Blue ? color.blue : LBlue ? color.aqua : Red ? #fb0707 : Orange ? color.orange : Yellow ? color.yellow : color.black
        # Priority order:
        # 1. GreenOnly (green_only)
        # 2. GreenRed (green_red but not green_only)
        # 3. Green (green but not green_only and not green_red)
        # 4. Blue
        # 5. LBlue
        # 6. Red
        # 7. Orange
        # 8. Yellow

        # Green conditions - highest priority
        signals.loc[green_only] = "GreenOnly"
        signals.loc[green_red & ~green_only] = "GreenRed"
        signals.loc[green & ~green_only & ~green_red] = "Green"

        # Other conditions - only apply where green is False (matching v1 elif logic)
        # Note: These are mutually exclusive with green due to bull/bear conditions,
        # but we check ~green to match v1's elif structure exactly
        signals.loc[blue & ~green] = "Blue"
        signals.loc[lblue & ~green] = "LBlue"
        signals.loc[red & ~green] = "Red"
        signals.loc[orange & ~green] = "Orange"
        signals.loc[yellow & ~green] = "Yellow"

        # Convert to list for compatibility with rest of code
        signals = signals.tolist()

        # Long Logic
        # fn_is_cd_condition_color(input_ca_cond_green,GreenOnly)[idx]
        # fn_is_cd_condition_color(input_ca_cond_blue,Blue)[idx]
        # fn_is_cd_condition_color(input_ca_cond_lblue,LBlue)[idx]
        # fn_is_cd_trend_condition(GreenOnly, Yellow, fix_ma_7, fix_ma_25)[idx]
        cal_long_1 = self.cal_is_cd_condition_color(
            isUseGreenOnly, signals, "GreenOnly"
        )
        cal_long_2 = self.cal_is_cd_condition_color(isUseBlue, signals, "Blue")
        cal_long_3 = self.cal_is_cd_condition_color(
            isUseLBlue, signals, "LBlue")
        cal_long_4 = self.cal_fn_cd_trend_condition(
            isUseLong, signals, fix_ma_7, fix_ma_25
        )

        # Short Logic
        # fn_is_cd_condition_color(input_ca_cond_red,Red)[idx]
        # fn_is_cd_condition_color(input_ca_cond_orange,Orange)[idx]
        # fn_is_cd_condition_color(input_ca_cond_yellow,Yellow)[idx]
        # fn_is_cd_condition_color(input_ca_cond_greenRed,GreenRed)[idx]
        cal_short_1 = self.cal_is_cd_condition_color(isUseRed, signals, "Red")
        cal_short_2 = self.cal_is_cd_condition_color(
            isUseOrange, signals, "Orange")
        cal_short_3 = self.cal_is_cd_condition_color(
            isUseYellow, signals, "Yellow")
        cal_short_greenred = self.cal_is_cd_condition_color(
            isUseGreenRed, signals, "GreenRed")
        cal_short_4 = self.cal_fn_cd_trend_condition_redGreen(
            isUseShort, signals)

        # Vectorized long, short, none signals
        # Note: cal_long_4 and cal_short_4 start from index 1, so we pad with False at index 0
        cal_long_1_series = pd.Series(cal_long_1)
        cal_long_2_series = pd.Series(cal_long_2)
        cal_long_3_series = pd.Series(cal_long_3)
        # Pad trend condition results: they start from index 1, so add False at index 0
        cal_long_4_padded = [
            False] + cal_long_4 if len(cal_long_4) < len(signals) else cal_long_4
        cal_long_4_series = pd.Series(cal_long_4_padded[:len(signals)])
        cal_short_1_series = pd.Series(cal_short_1)
        cal_short_2_series = pd.Series(cal_short_2)
        cal_short_3_series = pd.Series(cal_short_3)
        cal_short_greenred_series = pd.Series(cal_short_greenred)
        # Pad trend condition results: they start from index 1, so add False at index 0
        cal_short_4_padded = [
            False] + cal_short_4 if len(cal_short_4) < len(signals) else cal_short_4
        cal_short_4_series = pd.Series(cal_short_4_padded[:len(signals)])

        # Vectorized boolean operations
        long_conditions = cal_long_1_series | cal_long_2_series | cal_long_3_series | cal_long_4_series
        short_conditions = cal_short_1_series | cal_short_2_series | cal_short_3_series | cal_short_greenred_series | cal_short_4_series

        # Vectorized signal assignment
        resultSignals = pd.Series(["NONE"] * len(signals))
        resultSignals.loc[long_conditions] = "LONG"
        resultSignals.loc[short_conditions & ~long_conditions] = "SHORT"

        # Convert to list for compatibility
        resultSignals = resultSignals.tolist()

        # return json format
        # ============================================================
        # INDEX MAPPING: TradingView ↔ Python (after reverse)
        # ============================================================
        # TradingView Pine Script:        Python (after reverse):
        #   [0] = current bar (newest)  →  resultSignals[-1] = current bar
        #   [1] = previous bar         →  resultSignals[-2] = previous bar
        #
        # TradingView uses:
        #   - isSudden = true → idx_sudden = 0 (current bar)
        #   - longCondition = fn_is_long_condition(0) → uses [0]
        #   - shortCondition = fn_is_short_condition(0) → uses [0]
        #   - if longCondition and not inTrade → strategy.entry("enter long", ...)
        #   - if shortCondition and inTrade → strategy.close("enter long")
        # ============================================================
        # Index -1 = current bar (newest after reverse) = TradingView [0]
        confirm_idx = 1

        # TradingView entry/close logic:
        # - Entry: longCondition[0] and not inTrade → strategy.entry("enter long", ...) → position = "LONG"
        # - Close: shortCondition[0] and inTrade → strategy.close("enter long") → position = "SHORT"
        #
        # Python equivalent (using confirm_idx which maps to TradingView [0]):
        # - Entry: current_signal = "LONG" and prev_signal != "LONG" → position = "LONG"
        # - Close: current_signal = "SHORT" and prev_signal = "LONG" → position = "SHORT"

        position = "NONE"

        # TradingView entry/exit logic (lines 168-186):
        # Entry: if longCondition[0] and not inTrade → position = "LONG"
        # Exit:  if (shortCondition[0] or stopLossCondition) and inTrade → position = "SHORT"
        #
        # Simulate TradingView's inTrade state machine by processing bars sequentially
        # confirm_idx = 1 maps to TradingView [0] (current/newest bar)
        # We process from oldest to newest to build up the inTrade state

        in_trade = False  # TradingView's var inTrade = false

        # Process all bars sequentially to determine position state
        # This simulates TradingView's bar-by-bar processing
        for i in range(1, len(resultSignals)):  # Start from 1 to skip "NOW" marker
            current_long = resultSignals[i] == "LONG"
            current_short = resultSignals[i] == "SHORT"

            # Entry logic: if longCondition and not inTrade (TradingView line 168)
            if current_long and not in_trade:
                in_trade = True

            # Exit logic: if (shortCondition or stopLossCondition) and inTrade (TradingView line 177)
            # Note: stopLossCondition requires price tracking, so we only check shortCondition here
            elif current_short and in_trade:
                in_trade = False

        # Generate SigLabel array (L/S labels like TradingView lines 249-267)
        # TradingView: if order_buy[1] → label 'L', if order_sell[1] → label 'S' or 'SL'
        # order_buy := true when longCondition and not inTrade (entry)
        # order_sell := true when (shortCondition or stopLossCondition) and inTrade (exit)
        sig_short_labels = []
        in_trade_for_labels = False

        for i in range(len(resultSignals)):
            if i == 0:
                # Skip "NOW" marker
                sig_short_labels.append("")
                continue

            current_long = resultSignals[i] == "LONG"
            current_short = resultSignals[i] == "SHORT"

            label = ""

            # Entry logic: longCondition and not inTrade → order_buy := true → label 'L' (TradingView line 174, 249-254)
            if current_long and not in_trade_for_labels:
                in_trade_for_labels = True
                label = "L"  # Long entry label
            # Exit logic: (shortCondition or stopLossCondition) and inTrade → order_sell := true → label 'S' (TradingView line 185, 256-267)
            elif current_short and in_trade_for_labels:
                in_trade_for_labels = False
                # Short exit label (could be 'SL' for stop loss, but we don't track that here)
                label = "S"

            sig_short_labels.append(label)

        # Set current bar to None (TradingView convention)
        last_idx = len(resultSignals) - 1
        resultSignals[last_idx] = None
        signals[last_idx] = None
        sig_short_labels[last_idx] = None

        # Determine position based on previous bar's label (most reliable indicator)
        # Labels reflect actual entry/exit events: "L" = entry, "S" = exit
        # position = self._determine_position_from_labels(
        #     sig_short_labels, confirm_idx, resultSignals, in_trade
        # )

        if sig_short_labels[last_idx - 1] == "L":
            position = "LONG"
        elif sig_short_labels[last_idx - 1] == "S":
            position = "SHORT"
        else:
            position = "NONE"

        # Get timestamp from last kline (dict format from pool_signal_service)
        last_kline = kline[-1] if kline else {}
        last_timestamp = last_kline.get("open_time", 0) if isinstance(last_kline, dict) else (
            last_kline[0] if isinstance(last_kline, (list, tuple)) else 0)
        time_obj = datetime.fromtimestamp(
            last_timestamp / 1000) if last_timestamp else datetime.now()

        try:
            trend_condition = "UP"
            # Check if last_kline has 'close' key and fix_ma_25 is not empty
            if (isinstance(last_kline, dict) and "close" in last_kline and
                    len(fix_ma_25) > 0 and not pd.isna(fix_ma_25.iloc[-1])):
                close_price = float(last_kline["close"])
                ma_25_value = float(fix_ma_25.iloc[-1])
                if close_price < ma_25_value:
                    trend_condition = "DOWN"

                # log trend_condition and close and fix_ma_25 in one line
                logging.info(
                    "trend_condition: %s, close: %s, fix_ma_25: %s",
                    trend_condition, close_price, ma_25_value
                )
            else:
                # If data is not available, log warning and use default
                log.warning(
                    "Cannot calculate trend_condition: last_kline=%s, fix_ma_25 length=%d",
                    last_kline if isinstance(
                        last_kline, dict) else type(last_kline),
                    len(fix_ma_25) if hasattr(fix_ma_25, '__len__') else 0
                )
                trend_condition = "NONE"
        except Exception as e:
            logging.error(
                f"Error calculating trend_condition: {e}", exc_info=True)
            trend_condition = "NONE"

        if isDebug:
            result = {
                "sig": {
                    "position": position,
                    "trend": resultSignals[confirm_idx],
                    "candle": signals[confirm_idx],
                    "trend_condition": trend_condition,
                    "time": time_obj
                },
                "result_signals": resultSignals,
                "signals": signals,
                "sig_labels": sig_short_labels,
                "open": h_o,
                "high": h_h,
                "low": h_l,
                "close": h_c,
                "original_ema_7": original_ema_7,
                "original_ema_25": original_ema_25,
                "original_ema_50": original_ema_50,
                "timestamp": timestamp,
            }
        else:
            result = {
                "sig": {
                    "position": position,
                    "trend": resultSignals[confirm_idx],
                    "candle": signals[confirm_idx],
                    "trend_condition": trend_condition,
                    "time": time_obj
                },
            }
        return result

    def _determine_position_from_labels(
        self, sig_short_labels: list, confirm_idx: int, resultSignals: list, in_trade: bool
    ) -> str:
        """
        Determine trading position based on previous bar's label or current signal conditions.

        Priority:
        1. Previous bar's label ("L" = LONG entry, "S" = SHORT exit)
        2. Current bar signal conditions (if no label available)

        Args:
            sig_short_labels: List of signal labels ("L", "S", or "")
            confirm_idx: Index of the bar to confirm (maps to TradingView [0])
            resultSignals: List of signal values ("LONG", "SHORT", or None)
            in_trade: Whether currently in a trade based on state machine

        Returns:
            Position string: "LONG", "SHORT", or "NONE"
        """
        # First, check if we have a previous bar's label (most reliable)
        if len(sig_short_labels) >= 2:
            previous_label = sig_short_labels[len(sig_short_labels) - 2]
            if previous_label == "L":
                return "LONG"
            elif previous_label == "S":
                return "SHORT"

        # Fallback: determine position from current bar signal conditions
        # Entry: longCondition[0] and not inTrade (TradingView line 168)
        # Exit: shortCondition[0] and inTrade (TradingView line 177)
        if confirm_idx < len(resultSignals):
            is_long_signal = resultSignals[confirm_idx] == "LONG"
            is_short_signal = resultSignals[confirm_idx] == "SHORT"

            if is_long_signal and not in_trade:
                return "LONG"
            elif is_short_signal and in_trade:
                return "SHORT"
            elif in_trade:
                return "LONG"

        return "NONE"

    def cal_is_cd_condition_color(self, input_bool, bar_color, str_color=None):
        # Vectorized using pandas Series
        bar_color_series = pd.Series(bar_color)
        results = (input_bool & (bar_color_series == str_color)).tolist()
        return results

    def cal_fn_cd_trend_condition(self, input_bool, signals, ema_fast, ema_slow):
        # Vectorized using pandas Series with shift operation
        signals_series = pd.Series(signals)
        # Fill NaN values with 0 to prevent comparison errors
        ema_fast_series = pd.Series(ema_fast).fillna(0)
        ema_slow_series = pd.Series(ema_slow).fillna(0)

        # Shift signals by 1 to compare with previous
        prev_yellow = signals_series.shift(1) == "Yellow"
        curr_green_only = signals_series == "GreenOnly"
        ema_condition = ema_fast_series > ema_slow_series

        # Vectorized condition check
        results = (input_bool & prev_yellow &
                   curr_green_only & ema_condition).tolist()

        # Remove first element (index 0) since we start from index 1
        return results[1:] if len(results) > 1 else []

    def cal_fn_cd_trend_condition_redGreen(self, input_bool, signals):
        # Vectorized using pandas Series with shift operation
        signals_series = pd.Series(signals)

        # Shift signals by 1 to compare with previous
        prev_green_red = signals_series.shift(1) == "GreenRed"
        curr_yellow = signals_series == "Yellow"

        # Vectorized condition check
        results = (input_bool & curr_yellow & prev_green_red).tolist()

        # Remove first element (index 0) since we start from index 1
        return results[1:] if len(results) > 1 else []

    def calc_heikinashi(self, kline):
        """
        Calculate Heikin-Ashi candles from kline data.

        Args:
            kline: List of kline dicts with keys: open_time, open, high, low, close, volume, closed
                  OR list of lists/arrays with [timestamp, open, high, low, close, volume, ...]

        Returns:
            Tuple of (timestamp, ha_open, ha_high, ha_low, ha_close) lists
        """
        # Handle dict format from pool_signal_service
        if kline and isinstance(kline[0], dict):
            data = {
                "timestamp": [k["open_time"] for k in kline],
                "open": [float(k["open"]) for k in kline],
                "high": [float(k["high"]) for k in kline],
                "low": [float(k["low"]) for k in kline],
                "close": [float(k["close"]) for k in kline],
                "volume": [float(k["volume"]) for k in kline],
            }
            df = pd.DataFrame(data)
        else:
            # Handle list/array format (legacy support)
            df = pd.DataFrame(
                kline,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "end_time",
                    "x1",
                    "x2",
                    "x3",
                    "x4",
                    "x5",
                ],
            )
            # Convert OHLCV to float
            float_cols = ["open", "high", "low", "close", "volume"]
            for col in float_cols:
                df[col] = df[col].astype(float)

        # Calculate Heikin-Ashi
        ha = ta.ha(df["open"], df["high"], df["low"], df["close"])
        df = df.join(ha)

        # Find HA columns automatically
        ha_open_col = next(
            (c for c in df.columns if c.lower().startswith(
                "ha") and "open" in c.lower()),
            None
        )
        ha_high_col = next(
            (c for c in df.columns if c.lower().startswith(
                "ha") and "high" in c.lower()),
            None
        )
        ha_low_col = next(
            (c for c in df.columns if c.lower().startswith(
                "ha") and "low" in c.lower()),
            None
        )
        ha_close_col = next(
            (c for c in df.columns if c.lower().startswith(
                "ha") and "close" in c.lower()),
            None
        )

        if not all([ha_open_col, ha_high_col, ha_low_col, ha_close_col]):
            raise ValueError("Could not find Heikin-Ashi columns in DataFrame")

        # เพิ่ม original ema 7, 25, 50
        df["original_ema_7"] = ta.ema(pd.Series(df["close"]), 7)
        df["original_ema_25"] = ta.ema(pd.Series(df["close"]), 25)
        df["original_ema_50"] = ta.ema(pd.Series(df["close"]), 50)

        # แปลง NaN เป็น None เพื่อให้ JSON serializable
        # EMA อาจมี NaN เมื่อข้อมูลไม่เพียงพอ (เช่น ข้อมูลน้อยกว่า period)
        def replace_nan_with_none(series):
            return [None if pd.isna(val) else val for val in series]

        return (
            df["timestamp"].tolist(),
            df[ha_open_col].tolist(),
            df[ha_high_col].tolist(),
            df[ha_low_col].tolist(),
            df[ha_close_col].tolist(),
            replace_nan_with_none(df["original_ema_7"]),
            replace_nan_with_none(df["original_ema_25"]),
            replace_nan_with_none(df["original_ema_50"]),
        )
