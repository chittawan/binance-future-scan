"""Signal Request Model"""
from pydantic import BaseModel, Field


class SignalRequest(BaseModel):
    """
    Signal Request Model for requesting trading signals.

    Attributes:
        symbol: Trading symbol (e.g., 'BTCUSDT', 'TURBOUSDT')
        interval: Candle interval (e.g., '1m', '5m', '30m', '1h')
        isDebug: Whether to return debug information in the response
    """
    symbol: str = Field(
        "TURBOUSDT", description="Trading symbol, e.g., 'BTCUSDT'")
    interval: str = "30m"
    isDebug: bool = False

    class Config:
        """Config for Signal Request Model"""
        json_schema_extra = {"example": {
            "symbol": "TURBOUSDT", "interval": "1h", "isDebug": False}}
