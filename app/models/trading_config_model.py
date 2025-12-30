from pydantic import BaseModel, Field
from typing import Literal, Union, List


class TradingConfigModel(BaseModel):
    # SYSTEM
    IS_NOTIFY: bool = False
    IS_SPOT_MODE: bool = False
    USER_NAME: str = Field("USER_NAME", min_length=0, max_length=20)

    # Amount
    AMOUNT_MODE: Literal["FIX", "PERCENT"] = "FIX"
    AMOUNT_VALUE: float = Field(6.0, gt=0)
    LEVERAGE: int = Field(1, ge=1, le=5)
    MARGIN_TYPE: Literal["ISOLATED", "CROSSED"] = "ISOLATED"
    POSITION_HEDGE_MODE: Literal["true", "false"] = "false"

    # Signal Candle
    # Accept either a single symbol string or a list of symbols
    SIGNAL_SYMBOL: Union[str, List[str]] = Field(...)
    SIGNAL_INTERVAL: str = "30m"
    SIGNAL_FAST_EMA_PERIOD: int = Field(7, gt=0)
    SIGNAL_SLOW_EMA_PERIOD: int = Field(25, gt=0)

    # Signal Switch
    USE_LONG: bool = True
    USE_SHORT: bool = True
    USE_GREEN_ONLY: bool = True
    USE_BLUE: bool = True
    USE_LBLUE: bool = True
    USE_RED: bool = True
    USE_ORANGE: bool = True
    USE_YELLOW: bool = True
    USE_GREEN_RED: bool = True

    # BOT
    SIGNAL_CANDLES: int = Field(80, gt=0)
    SLEEP_SECONDS: int = Field(5, ge=1)

    # BINANCE_BASEURL: str = "https://fapi.binance.com"
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""
    NTFY_URL: str = "https://ntfy.codewalk.myds.me/binance-notify"
    NTFY_TOKEN: str = "tk_44gewsus4n1sx0b3edaf7c144nn92"
