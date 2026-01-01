__all__ = ["TradingConfigService", "trading_config_service"]

import logging
import asyncio
from threading import Lock
from app.models.trading_config_model import TradingConfigModel
import json
from pathlib import Path

CONFIG_PATH = Path("trading_config.json")


class TradingConfigService:
    def __init__(self):
        logging.info("Initializing TradingConfigService")
        self._lock = Lock()
        self._config = TradingConfigModel(SIGNAL_SYMBOL="NONE")
        self._restart_task = None  # Track pending restart task
        self._restart_delay = 2.0  # Debounce delay in seconds
        self.load_config()

    def get(self) -> TradingConfigModel:
        with self._lock:
            logging.debug("Getting trading config snapshot")
            return self._config.model_copy()

    def update(self, payload: dict):
        # Import here to avoid circular import
        logging.info("Updating trading config: %s", payload)

    def info(self):
        cfg = self.get()  # snapshot

        logging.info("=" * 60)
        logging.info("BOT RUNNER ENVIRONMENT CONFIGURATION")
        logging.info("=" * 60)
        logging.info("BOT AMOUNT")
        logging.info("=" * 60)
        logging.info("BOT Signal Candle")
        logging.info("=" * 60)
        logging.info("SYMBOL: %s", cfg.SIGNAL_SYMBOL)
        logging.info("INTERVAL: %s", cfg.SIGNAL_INTERVAL)
        logging.info("SIGNAL_FAST_EMA_PERIOD: %s", cfg.SIGNAL_FAST_EMA_PERIOD)
        logging.info("SIGNAL_SLOW_EMA_PERIOD: %s", cfg.SIGNAL_SLOW_EMA_PERIOD)
        logging.info("=" * 60)

    def save_config(self):
        CONFIG_PATH.write_text(
            json.dumps(self._config.model_dump(), indent=2), encoding="utf-8"
        )
        logging.info("Trading config saved : %s", CONFIG_PATH)

    def load_config(self):
        if CONFIG_PATH.exists():
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            # Remove old BINANCE_FUTURE_API_KEY and BINANCE_FUTURE_API_SECRET if present
            if "BINANCE_FUTURE_API_KEY" in data:
                del data["BINANCE_FUTURE_API_KEY"]
            if "BINANCE_FUTURE_API_SECRET" in data:
                del data["BINANCE_FUTURE_API_SECRET"]
            # Ensure CLIENT_CONFIGS exists (for backward compatibility)
            if "CLIENT_CONFIGS" not in data:
                data["CLIENT_CONFIGS"] = []
            self._config = TradingConfigModel(**data)
            self.save_config()


trading_config_service: TradingConfigService = TradingConfigService()
