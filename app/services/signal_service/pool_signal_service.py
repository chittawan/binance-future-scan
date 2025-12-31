"""Pool signal service for managing Binance futures trading signals.

This module provides a service for monitoring and processing trading signals
from Binance futures markets via WebSocket connections and REST API calls.
It manages kline data, calculates trading signals, and coordinates with
bot runner services for signal execution.
"""
import aiohttp
import asyncio
import logging
import time
from collections import defaultdict, deque
from datetime import datetime

from app.models.trading_config_model import TradingConfigModel
from app.services.signal_service.scan_signal_service_v2 import ScanSignalServiceV2

log = logging.getLogger(__name__.split(".")[-1])

_pool_signal_service: "PoolSignalService | None" = None


def get_pool_signal_service(
    config: TradingConfigModel | None = None,
    limit: int = 100,
) -> "PoolSignalService":
    global _pool_signal_service

    # If service doesn't exist and config is None, try to get it from trading_config_service
    if _pool_signal_service is None and config is None:
        try:
            from app.services.trading_config_service import trading_config_service
            config = trading_config_service.get()
        except Exception as e:
            log.warning(
                "Failed to get config from trading_config_service: %s", e)
            # If we can't get config, we can't create the service
            # Return None or raise an error - for now, we'll raise
            raise ValueError(
                "Cannot create PoolSignalService without config. Please provide config or ensure trading_config_service is initialized.")

    if _pool_signal_service is None:
        if config is None:
            raise ValueError("Cannot create PoolSignalService: config is None")
        _pool_signal_service = PoolSignalService(config, limit)
    elif config is not None:
        # Update config if instance already exists
        _pool_signal_service.update_config(config, limit)

    return _pool_signal_service


def reset_pool_signal_service():
    """Reset the singleton instance. Used when stopping the service."""
    global _pool_signal_service
    _pool_signal_service = None


class PoolSignalService:
    BASE_REST = "https://fapi.binance.com"
    BASE_WS = "wss://fstream.binance.com/ws"
    MAX_BAR = 100  # Maximum number of bars to keep
    WS_IDLE_TIMEOUT = 60  # ถ้าไม่มี msg เกิน 60s → reconnect
    WS_RECONNECT_DELAY = 5  # Delay before reconnecting
    WS_HEARTBEAT = 20  # WebSocket heartbeat interval

    # HTTP Timeout Constants
    HTTP_CONNECT_TIMEOUT = 10  # seconds - time to wait for connection establishment
    HTTP_READ_TIMEOUT = 60  # seconds - time to wait for response data
    HTTP_TOTAL_TIMEOUT = 60  # seconds - total request time limit

    # Initial Load Constants
    INITIAL_LOAD_CONCURRENCY = 5  # Maximum concurrent requests during initial load
    INITIAL_LOAD_BATCH_DELAY = 1  # Delay between batches (seconds)
    INITIAL_LOAD_RETRY_MAX_ATTEMPTS = 5  # Maximum retry attempts for 429
    # Base delay for exponential backoff (seconds)
    INITIAL_LOAD_RETRY_BASE_DELAY = 1.0

    def __init__(self, config: TradingConfigModel, limit: int = 100):

        self.config = config
        # Ensure limit doesn't exceed MAX_BAR
        self.limit = min(limit, self.MAX_BAR)

        # cache: {symbol_interval: deque[klines]}
        # deque with maxlen automatically maintains only MAX_BAR items
        self.klines = defaultdict(lambda: deque(maxlen=self.MAX_BAR))

        self._ws_tasks: list[asyncio.Task] = []
        self._scan_signal_service = ScanSignalServiceV2()
        # Track minute updates for summary logging
        self._minute_updates: dict[int, set[str]] = {}
        self._last_logged_minute: int | None = None
        self._minute_start_times: dict[int, float] = {}
        # Track when we last logged (to ensure 1 min interval)
        self._last_log_timestamp: float = time.time()
        # Symbols will be fetched from Binance exchangeInfo
        self.symbols: list[str] = []
        self.total_symbols = 0
        # Track process locks to prevent duplicate signal processing
        # Format: {f"{symbol}{signal_time}": asyncio.Lock}
        self._process_locks: dict[str, asyncio.Lock] = {}
        # Lock to safely access _process_locks dict
        self._process_locks_lock = asyncio.Lock()
        # Status tracking for initialization
        # idle, wait_for_fetch_symbol, wait_for_rest, wait_for_ws, completed
        self._status: str = "idle"
        self._start_task: asyncio.Task | None = None

    def _interval_ms(self) -> int:
        m = self.config.SIGNAL_INTERVAL
        if m.endswith("m"):
            return int(m[:-1]) * 60_000
        if m.endswith("h"):
            return int(m[:-1]) * 60 * 60_000
        raise ValueError(f"Unsupported interval {m}")
    # =========================================================
    # Public entry
    # =========================================================

    async def start(self):
        log.info("🚀 Starting PoolSignalService")

        # Ensure we're not already running
        if self.is_running():
            log.warning(
                "⚠ PoolSignalService is already running, stopping first...")
            await self.stop()

        # Cancel existing start task if any
        if self._start_task is not None and not self._start_task.done():
            self._start_task.cancel()
            try:
                await self._start_task
            except asyncio.CancelledError:
                pass

        # Start initialization as background task
        self._start_task = asyncio.create_task(self._start_background())

    async def _start_background(self):
        """Background task that runs initialization steps with status tracking."""
        try:
            # Fetch symbols from Binance exchangeInfo
            self._status = "wait_for_fetch_symbol"
            log.info("📡 Status: %s", self._status)
            await self.fetch_symbols()

            # Initial load (REST)
            self._status = "wait_for_rest"
            log.info("📡 Status: %s", self._status)
            await self.initial_load()

            # Start WebSocket connections
            self._status = "wait_for_ws"
            log.info("📡 Status: %s", self._status)
            await self.start_ws()

            # Completed
            self._status = "completed"
            log.info(
                "✅ Status: %s - PoolSignalService initialization complete", self._status)
        except asyncio.CancelledError:
            log.info("🛑 Start background task cancelled")
            self._status = "idle"
            raise
        except Exception as e:  # noqa: BLE001
            log.error("❌ Error during initialization: %s", e, exc_info=True)
            self._status = "idle"
            raise

    def get_status(self) -> str:
        """Get the current initialization status."""
        return self._status

    # =========================================================
    # Fetch symbols from Binance
    # =========================================================
    async def fetch_symbols(self):
        """Fetch all Binance Futures symbols from exchangeInfo endpoint.

        Filters symbols by:
        - contractType == "PERPETUAL"
        - status == "TRADING"
        - quoteAsset == "USDT"
        """
        log.info("📡 Fetching Binance Futures symbols from exchangeInfo...")

        url = f"{self.BASE_REST}/fapi/v1/exchangeInfo"

        timeout = aiohttp.ClientTimeout(
            total=self.HTTP_TOTAL_TIMEOUT,
            connect=self.HTTP_CONNECT_TIMEOUT
        )

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
        except aiohttp.ServerTimeoutError as e:
            log.error(
                "⏰ Server timeout fetching exchangeInfo (URL: %s): %s. "
                "Server did not respond within timeout period.",
                url, e
            )
            raise
        except aiohttp.ClientConnectionError as e:
            log.error(
                "❌ Connection error fetching exchangeInfo (URL: %s): %s. "
                "Network may be disconnected or server unreachable.",
                url, e
            )
            raise
        except aiohttp.ClientError as e:
            log.error(
                "❌ Network error fetching exchangeInfo (URL: %s): %s. "
                "Error type: %s",
                url, e, type(e).__name__
            )
            raise
        except asyncio.TimeoutError:
            log.error(
                "⏰ Timeout fetching exchangeInfo (URL: %s): "
                "connect_timeout=%ss, total_timeout=%ss. "
                "This may occur during network interruptions.",
                url, self.HTTP_CONNECT_TIMEOUT, self.HTTP_TOTAL_TIMEOUT
            )
            raise
        except Exception as e:  # noqa: BLE001
            log.error(
                "❌ Unexpected error fetching exchangeInfo (URL: %s): %s. "
                "Error type: %s",
                url, e, type(e).__name__
            )
            raise

        # Filter symbols
        symbols = []
        for symbol_info in data.get("symbols", []):
            if (
                symbol_info.get("contractType") == "PERPETUAL"
                and symbol_info.get("status") == "TRADING"
                and symbol_info.get("quoteAsset") == "USDT"
            ):
                symbols.append(symbol_info["symbol"])

        self.symbols = sorted(symbols)  # Sort for consistency
        self.total_symbols = len(self.symbols)

        log.info(
            "✅ Fetched %d Binance Futures symbols (PERPETUAL, TRADING, USDT)",
            self.total_symbols
        )

        if self.total_symbols == 0:
            log.warning("⚠️ No symbols found! Check exchangeInfo response.")

    # =========================================================
    # Initial load (REST)
    # =========================================================
    async def initial_load(self):
        log.info("Initial loading klines for %d symbols...", len(self.symbols))

        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.INITIAL_LOAD_CONCURRENCY)

        timeout = aiohttp.ClientTimeout(
            total=self.HTTP_TOTAL_TIMEOUT,
            connect=self.HTTP_CONNECT_TIMEOUT
        )

        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Process symbols in batches with delay
            batch_size = self.INITIAL_LOAD_CONCURRENCY
            total_batches = (len(self.symbols) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(self.symbols))
                batch_symbols = self.symbols[start_idx:end_idx]

                log.info(
                    "Loading batch %d/%d: %d symbols (from %s to %s)",
                    batch_idx + 1,
                    total_batches,
                    len(batch_symbols),
                    batch_symbols[0] if batch_symbols else "N/A",
                    batch_symbols[-1] if batch_symbols else "N/A"
                )

                # Create tasks for this batch with semaphore
                tasks = [
                    self._load_symbol_with_semaphore(
                        session, semaphore, symbol)
                    for symbol in batch_symbols
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

                # Add delay between batches (except for the last batch)
                if batch_idx < total_batches - 1:
                    await asyncio.sleep(self.INITIAL_LOAD_BATCH_DELAY)

        log.info("Initial load completed")

    async def _load_symbol_with_semaphore(
        self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, symbol: str
    ):
        """Load symbol with semaphore to limit concurrency."""
        async with semaphore:
            await self._load_symbol(session, symbol)

    async def _load_symbol(self, session: aiohttp.ClientSession, symbol: str):
        """Load klines for a symbol with retry logic for 429 rate limits."""
        url = f"{self.BASE_REST}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": self.config.SIGNAL_INTERVAL,
            "limit": self.limit,
        }

        data = None
        # Retry logic for 429 rate limit errors
        for attempt in range(1, self.INITIAL_LOAD_RETRY_MAX_ATTEMPTS + 1):
            try:
                async with session.get(url, params=params) as resp:
                    # Handle 429 rate limit specifically
                    if resp.status == 429:
                        # Try to get retry-after header
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            wait_time = float(retry_after)
                        else:
                            # Exponential backoff: base_delay * (2 ^ (attempt - 1))
                            wait_time = self.INITIAL_LOAD_RETRY_BASE_DELAY * \
                                (2 ** (attempt - 1))

                        if attempt < self.INITIAL_LOAD_RETRY_MAX_ATTEMPTS:
                            log.warning(
                                "⏸️ Rate limit (429) for %s, attempt %d/%d, waiting %.2fs",
                                symbol, attempt, self.INITIAL_LOAD_RETRY_MAX_ATTEMPTS, wait_time
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            log.error(
                                "❌ Rate limit (429) for %s after %d attempts, giving up",
                                symbol, self.INITIAL_LOAD_RETRY_MAX_ATTEMPTS
                            )
                            resp.raise_for_status()

                    resp.raise_for_status()
                    data = await resp.json()
                    break  # Success, exit retry loop

            except aiohttp.ServerTimeoutError as e:
                log.error(
                    "⏰ Server timeout loading klines for %s (URL: %s): %s. "
                    "Server did not respond within timeout period.",
                    symbol, url, e
                )
                raise
            except aiohttp.ClientConnectionError as e:
                log.error(
                    "❌ Connection error loading klines for %s (URL: %s): %s. "
                    "Network may be disconnected or server unreachable.",
                    symbol, url, e
                )
                raise
            except aiohttp.ClientResponseError as e:
                # Handle other HTTP errors (but not 429, which is handled above)
                if e.status == 429:
                    # This shouldn't happen if we handled it above, but just in case
                    if attempt < self.INITIAL_LOAD_RETRY_MAX_ATTEMPTS:
                        wait_time = self.INITIAL_LOAD_RETRY_BASE_DELAY * \
                            (2 ** (attempt - 1))
                        log.warning(
                            "⏸️ Rate limit (429) for %s (from exception), attempt %d/%d, waiting %.2fs",
                            symbol, attempt, self.INITIAL_LOAD_RETRY_MAX_ATTEMPTS, wait_time
                        )
                        await asyncio.sleep(wait_time)
                        continue
                log.error(
                    "❌ HTTP error loading klines for %s (URL: %s): %s (status: %d)",
                    symbol, url, e, e.status
                )
                raise
            except aiohttp.ClientError as e:
                log.error(
                    "❌ Network error loading klines for %s (URL: %s): %s. "
                    "Error type: %s",
                    symbol, url, e, type(e).__name__
                )
                raise
            except asyncio.TimeoutError:
                log.error(
                    "⏰ Timeout loading klines for %s (URL: %s): "
                    "connect_timeout=%ss, total_timeout=%ss. "
                    "This may occur during network interruptions.",
                    symbol, url, self.HTTP_CONNECT_TIMEOUT, self.HTTP_TOTAL_TIMEOUT
                )
                raise
            except Exception as e:  # noqa: BLE001
                log.error(
                    "❌ Unexpected error loading klines for %s (URL: %s): %s. "
                    "Error type: %s",
                    symbol, url, e, type(e).__name__
                )
                raise

        # Ensure data was successfully loaded
        if data is None:
            raise ValueError(
                f"Failed to load klines for {symbol} after all retry attempts")

        key = self._get_key(symbol)

        for k in data:
            self.klines[key].append(self._normalize_kline(k))

        # Ensure we don't exceed MAX_BAR after initial load
        buf = self.klines[key]
        if len(buf) > self.MAX_BAR:
            log.warning("Initial load exceeded MAX_BAR for %s: %d > %d, truncating", key, len(
                buf), self.MAX_BAR)
            # Keep only the last MAX_BAR items
            while len(buf) > self.MAX_BAR:
                buf.popleft()

        log.info("Loaded %s klines for %s (max: %s)", len(
            self.klines[key]), key, self.MAX_BAR)

    # =========================================================
    # WebSocket
    # =========================================================
    async def start_ws(self):
        log.info("Starting WebSocket listeners")

        for symbol in self.symbols:
            task = asyncio.create_task(self._ws_listen(symbol))
            self._ws_tasks.append(task)

        # Start periodic timeout checker for minute updates
        timeout_checker = asyncio.create_task(self._periodic_timeout_check())
        self._ws_tasks.append(timeout_checker)

    async def _ws_listen(self, symbol: str):
        stream = f"{symbol.lower()}@kline_{self.config.SIGNAL_INTERVAL}"
        url = f"{self.BASE_WS}/{stream}"

        try:
            while True:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.ws_connect(
                            url,
                            heartbeat=self.WS_HEARTBEAT,
                            receive_timeout=self.WS_IDLE_TIMEOUT,  # ⭐ สำคัญมาก - enforce timeout
                        ) as ws:
                            log.info("🟢 WS connected: %s", symbol)

                            while True:
                                try:
                                    msg = await ws.receive(timeout=self.WS_IDLE_TIMEOUT)
                                except asyncio.TimeoutError:
                                    log.warning(
                                        "⏰ WS idle timeout %ss (%s), reconnecting",
                                        self.WS_IDLE_TIMEOUT, symbol
                                    )
                                    break

                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    try:
                                        data = msg.json()
                                        if data is None:
                                            log.warning(
                                                "⚠️ Received None data from WS for %s", symbol)
                                            continue
                                        if "k" not in data:
                                            log.warning(
                                                "⚠️ WS message for %s missing 'k' key. Data: %s",
                                                symbol, data
                                            )
                                            continue
                                        kline_data = data.get("k")
                                        if kline_data is None:
                                            log.warning(
                                                "⚠️ WS message for %s has None 'k' value", symbol)
                                            continue
                                        await self.on_kline_update(
                                            symbol,
                                            kline_data
                                        )
                                    except (ValueError, KeyError, TypeError) as e:
                                        log.warning(
                                            "⚠️ Failed to parse WS message for %s: %s. Message: %s",
                                            symbol, e, msg.data if hasattr(
                                                msg, 'data') else 'N/A'
                                        )
                                        continue
                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.ERROR,
                                ):
                                    log.warning(
                                        "🔴 WS closed (%s): %s", symbol, msg.type)
                                    break

                except aiohttp.ClientConnectionError as e:
                    log.error(
                        "❌ WebSocket connection error for %s (URL: %s): %s. "
                        "Network may be disconnected or server unreachable.",
                        symbol, url, e
                    )
                except aiohttp.ClientError as e:
                    log.error(
                        "❌ WebSocket client error for %s (URL: %s): %s. "
                        "Error type: %s",
                        symbol, url, e, type(e).__name__
                    )
                except asyncio.TimeoutError:
                    log.error(
                        "⏰ WebSocket connection timeout for %s (URL: %s). "
                        "Network may be slow or disconnected.",
                        symbol, url
                    )
                except asyncio.CancelledError:
                    log.info("🛑 WS listener cancelled for %s", symbol)
                    raise
                except Exception as e:  # noqa: BLE001
                    log.error(
                        "❌ WS exception for %s (URL: %s): %s. "
                        "Error type: %s",
                        symbol, url, e, type(e).__name__
                    )

                log.info("🔄 Reconnecting WS %s in %ss...",
                         symbol, self.WS_RECONNECT_DELAY)
            try:
                await asyncio.sleep(self.WS_RECONNECT_DELAY)
            except asyncio.CancelledError:
                log.info("🛑 WS listener cancelled for %s during reconnect", symbol)
                raise
        except asyncio.CancelledError:
            log.info("🛑 WS listener stopped for %s", symbol)
            raise

    # =========================================================
    # Kline update (from WS)
    # =========================================================
    async def on_kline_update(self, symbol: str, kline: dict):
        try:
            ws_cur_line = self._normalize_ws_kline(kline)
        except (ValueError, KeyError, TypeError) as e:
            log.error(
                "❌ Failed to normalize kline for %s: %s. kline=%s",
                symbol, e, kline
            )
            return

        key = self._get_key(symbol)
        buf = self.klines[key]

        # Handle empty buffer case
        if not buf:
            log.debug("🆕 First kline %s %s %s", key,
                      symbol, ws_cur_line["open_time"])
            buf.append(ws_cur_line)
            return

        mem_cur_line = buf[-1]
        interval_ms = self._interval_ms()

        # Update existing kline with same open_time
        if mem_cur_line["open_time"] == ws_cur_line["open_time"]:
            # Validate timestamp sequence if we have previous kline
            if len(buf) >= 2:
                mem_prev_line = buf[-2]
                expected_prev_time = ws_cur_line["open_time"] - interval_ms
                if mem_prev_line["open_time"] != expected_prev_time:
                    await self.resync_rest(symbol)
                    return

            buf[-1] = ws_cur_line
            # Track update for minute summary logging
            # log.info("🔄 Kline updated %s %s", symbol, ws_cur_line["open_time"])
            self._track_minute_update(symbol, ws_cur_line["open_time"])
        else:
            # New kline - validate timestamp sequence
            expected_open_time = mem_cur_line["open_time"] + interval_ms

            if ws_cur_line["open_time"] != expected_open_time:
                await self.resync_rest(symbol)
                return

            buf.append(ws_cur_line)
            # deque with maxlen automatically maintains only MAX_BAR items
            # Track update for minute summary logging
            # log.info("🔄 Kline new %s %s", symbol, ws_cur_line["open_time"])
            self._track_minute_update(symbol, ws_cur_line["open_time"])

        # Ensure we never exceed MAX_BAR (safety check)
        # This shouldn't happen with deque(maxlen), but just in case
        if len(buf) > self.MAX_BAR:
            while len(buf) > self.MAX_BAR:
                buf.popleft()

        # 🧠 Detect candle lag - ถ้า open_time ล่าสุด < now - interval * 2 → resync REST
        current_time_ms = int(time.time() * 1000)
        interval_ms = self._interval_ms()
        if buf and current_time_ms - buf[-1]["open_time"] > 2 * interval_ms:
            log.warning(
                "⏰ Candle lag detected for %s: last open_time %s, now %s (diff: %sms), resyncing",
                symbol, buf[-1]["open_time"], current_time_ms,
                current_time_ms - buf[-1]["open_time"]
            )
            await self.resync_rest(symbol)
            return

        # ✅ Calculate signal only when candle closed
        if ws_cur_line["closed"]:
            await self.calculate_signal(symbol)

    # =========================================================
    # Signal hook
    # =========================================================
    async def calculate_signal(self, symbol: str):
        log.debug("Calculating signal for %s", symbol)
        key = self._get_key(symbol)
        buf = self.klines[key]

        # list self.klines prop keys
        # log.info("🔍 self.klines keys: %s", self.klines.keys())

        if len(buf) < 2:
            log.warning(
                "Not enough klines for %s: have %d, need at least 2", symbol, len(buf))
            return

        closed_candle = buf[-2]  # closed candle
        log.debug("📊 Signal candle %s %s (buffer size: %d)",
                  symbol, closed_candle["open_time"], len(buf))

        # Convert deque to list for candle_service
        kline_list = list(buf)
        signals = await self._scan_signal_service.scan(symbol=symbol, kline_list=kline_list, config=self.config)
        log.info("🔍 Signals for %s: %s", symbol, signals)

        # Validate signals is not None
        if signals is None:
            log.warning(
                "⚠️ scan() returned None for %s, skipping signal processing", symbol)
            return

        sig_time = signals.get("sig", {}).get("time")
        if not sig_time:
            log.warning(
                "No signal time found for %s, skipping processing", symbol)
            return

        # Create process lock key: {symbol}{signal_time} to prevent duplicate processing
        lock_key = f"{symbol}{sig_time}"

        # Get or create lock for this symbol+signal_time combination
        async with self._process_locks_lock:
            if lock_key not in self._process_locks:
                self._process_locks[lock_key] = asyncio.Lock()
            process_lock = self._process_locks[lock_key]

        # Use lock to prevent concurrent processing of the same signal
        async with process_lock:
            logging.info(
                "🔍 Processing signal alert to bot client %s: %s", symbol, signals["sig"]["type"])

        # Clean up old locks (older than 1 hour) to prevent memory leak
        # This is done outside the lock to avoid blocking
        if len(self._process_locks) > 100:  # Only cleanup if dict is getting large
            async with self._process_locks_lock:
                # Remove locks that are no longer needed (keep only recent 50)
                if len(self._process_locks) > 50:
                    # Keep the most recent locks
                    keys_to_remove = list(self._process_locks.keys())[:-50]
                    for key in keys_to_remove:
                        self._process_locks.pop(key, None)

    # =========================================================
    # Minute summary logging
    # =========================================================
    async def _periodic_timeout_check(self):
        """Periodically check and log summary every 1 minute"""
        try:
            while True:
                try:
                    await asyncio.sleep(10)  # Check every 10 seconds
                except asyncio.CancelledError:
                    log.info("🛑 Periodic timeout checker cancelled")
                    raise

                current_time = time.time()
                current_time_ms = int(current_time * 1000)
                interval_ms = self._interval_ms()

                # Check if current time is at the start of a new minute (ends with 00 seconds)
                current_minute = int(current_time) // 60
                last_log_minute = int(self._last_log_timestamp) // 60

                # Log at the start of each minute (when minute changes)
                if current_minute > last_log_minute:
                    # Find the most recent open_time to log
                    if self._minute_start_times:
                        # Filter out None values and get the most recent open_time
                        valid_open_times = [
                            t for t in self._minute_start_times.keys() if t is not None]
                        if valid_open_times:
                            most_recent_open_time = max(valid_open_times)
                            # Log summary for this minute (only once per minute)
                            self._log_minute_summary_if_ready(
                                most_recent_open_time)

                # Clean up old minutes that are more than 2 intervals old
                for open_time in list(self._minute_start_times.keys()):
                    if current_time_ms - open_time > 2 * interval_ms:
                        self._cleanup_old_minutes(open_time)
        except asyncio.CancelledError:
            log.info("🛑 Periodic timeout checker stopped")
            raise

    def _track_minute_update(self, symbol: str, open_time: int):
        """Track symbol update for a specific minute (open_time)"""
        # Validate open_time is not None
        if open_time is None:
            log.error(
                "❌ Invalid open_time (None) for %s, skipping update", symbol)
            return

        if open_time not in self._minute_updates:
            self._minute_updates[open_time] = set()
            self._minute_start_times[open_time] = time.time()

        self._minute_updates[open_time].add(symbol)
        self._log_minute_summary_if_ready(open_time)

    def _log_minute_summary_if_ready(self, open_time: int):
        """Log summary every 1 minute (at :00 seconds) or when all symbols are updated"""
        # Validate open_time is not None
        if open_time is None:
            log.error(
                "❌ Invalid open_time (None) in _log_minute_summary_if_ready, skipping")
            return

        updated = self._minute_updates.get(open_time, set())
        current_time = time.time()

        # Check if we're at the start of a new minute (minute has changed)
        # Validate _last_log_timestamp is not None
        if self._last_log_timestamp is None:
            log.warning(
                "⚠️ _last_log_timestamp is None, initializing to current time")
            self._last_log_timestamp = current_time

        current_minute = int(current_time) // 60
        last_log_minute = int(self._last_log_timestamp) // 60

        # Skip if still in the same minute
        if current_minute <= last_log_minute:
            return

        all_complete = len(updated) == self.total_symbols

        # Log at the start of each minute (when minute changes)
        should_log = True

        if should_log:
            # Format open_time to readable time string
            minute_str = datetime.fromtimestamp(
                open_time / 1000).strftime("%H:%M")

            if all_complete:
                log.info(
                    "🔄 upkline %s total %d/%d",
                    minute_str,
                    len(updated),
                    self.total_symbols,
                )
            else:
                missing = set(self.symbols) - updated
                log.warning(
                    "⚠ upkline %s incomplete %d/%d missing=%s",
                    minute_str,
                    len(updated),
                    self.total_symbols,
                    ",".join(sorted(missing)),
                )
            self._last_logged_minute = open_time
            self._last_log_timestamp = time.time()  # Update last log timestamp
            self._cleanup_old_minutes(open_time)

    def _cleanup_old_minutes(self, current_open_time: int):
        """Clean up old minute tracking data"""
        for t in list(self._minute_updates.keys()):
            if t < current_open_time:
                del self._minute_updates[t]
                if t in self._minute_start_times:
                    del self._minute_start_times[t]

    # =========================================================
    # Utils
    # =========================================================
    def _get_key(self, symbol: str) -> str:
        return f"{symbol}_{self.config.SIGNAL_INTERVAL}"

    def _normalize_kline(self, k):
        return {
            "open_time": k[0],
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "closed": True,
        }

    def _normalize_ws_kline(self, k):
        # Validate that k is not None
        if k is None:
            raise ValueError("Invalid kline data: k is None")

        # Validate that k is a dict
        if not isinstance(k, dict):
            raise ValueError(
                f"Invalid kline data: k is not a dict, got {type(k).__name__}. kline={k}")

        # Validate that timestamp is not None
        if k.get("t") is None:
            raise ValueError(
                f"Invalid kline data: timestamp is None. kline={k}")
        return {
            "open_time": k["t"],
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "closed": k["x"],
        }

    def is_running(self) -> bool:
        return len(self._ws_tasks) > 0

    async def stop(self):
        log.info("🛑 Stopping PoolSignalService")

        # Cancel start background task if running
        if self._start_task is not None and not self._start_task.done():
            self._start_task.cancel()
            try:
                await self._start_task
            except asyncio.CancelledError:
                pass
            self._start_task = None

        # Cancel all tasks
        for task in self._ws_tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to finish (with timeout)
        if self._ws_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._ws_tasks, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                log.warning(
                    "⚠ Some tasks did not stop within timeout, forcing cleanup")

        self._ws_tasks.clear()
        self.klines.clear()
        self._minute_updates.clear()
        self._minute_start_times.clear()
        self._process_locks.clear()
        self._status = "idle"
        log.info("✅ PoolSignalService stopped")

    def update_config(self, config: TradingConfigModel, limit: int = 100):
        """Update config and related settings. Should be called before start() after stop()."""
        # log.info("🔄 Updating PoolSignalService config")
        self.config = config
        self.limit = min(limit, self.MAX_BAR)
        # Symbols will be fetched in start(), so reset here
        self.symbols = []
        self.total_symbols = 0
        # Clear old data to prepare for new config
        self.klines.clear()
        self._minute_updates.clear()
        self._minute_start_times.clear()
        self._process_locks.clear()
        # Reset status
        self._status = "idle"
        # Ensure _last_log_timestamp is initialized
        if not hasattr(self, '_last_log_timestamp') or self._last_log_timestamp is None:
            self._last_log_timestamp = time.time()
        # log.info("✅ PoolSignalService config updated")

    async def resync_rest(self, symbol: str):
        key = self._get_key(symbol)
        buf = self.klines[key]

        last_open_time = buf[-1]["open_time"] if buf else None

        log.info("🔄 REST resync %s from %s", symbol, last_open_time)

        url = f"{self.BASE_REST}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": self.config.SIGNAL_INTERVAL,
            "limit": self.limit,
        }

        timeout = aiohttp.ClientTimeout(
            total=self.HTTP_TOTAL_TIMEOUT,
            connect=self.HTTP_CONNECT_TIMEOUT
        )
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
        except aiohttp.ServerTimeoutError as e:
            log.error(
                "⏰ Server timeout during REST resync for %s (URL: %s): %s. "
                "Server did not respond within timeout period.",
                symbol, url, e
            )
            raise
        except aiohttp.ClientConnectionError as e:
            log.error(
                "❌ Connection error during REST resync for %s (URL: %s): %s. "
                "Network may be disconnected or server unreachable.",
                symbol, url, e
            )
            raise
        except aiohttp.ClientError as e:
            log.error(
                "❌ Network error during REST resync for %s (URL: %s): %s. "
                "Error type: %s",
                symbol, url, e, type(e).__name__
            )
            raise
        except asyncio.TimeoutError:
            log.error(
                "⏰ Timeout during REST resync for %s (URL: %s): "
                "connect_timeout=%ss, total_timeout=%ss. "
                "This may occur during network interruptions.",
                symbol, url, self.HTTP_CONNECT_TIMEOUT, self.HTTP_TOTAL_TIMEOUT
            )
            raise
        except Exception as e:  # noqa: BLE001
            log.error(
                "❌ Unexpected error during REST resync for %s (URL: %s): %s. "
                "Error type: %s",
                symbol, url, e, type(e).__name__
            )
            raise

        # Clear buffer and reload to avoid duplicates
        buf.clear()

        for k in data:
            normalized = self._normalize_kline(k)
            # Only add if it's new or if buffer is empty
            if not buf or normalized["open_time"] > buf[-1]["open_time"]:
                buf.append(normalized)

        buf[-1]["closed"] = False
        log.info("✅ Resync done %s (%s klines)", symbol, len(buf))

    def _is_kline_buffer_healthy(
        self,
        buf: deque,
        tolerance_intervals: int = 2,
    ) -> tuple[bool, str]:
        """
        Returns (is_healthy, reason)
        """
        if not buf:
            return False, "empty"

        interval_ms = self._interval_ms()
        now_ms = int(time.time() * 1000)

        # 1️⃣ check lag
        last_open = buf[-1]["open_time"]
        if now_ms - last_open > tolerance_intervals * interval_ms:
            return False, f"lagging last_open={last_open}"

        # 2️⃣ check gap
        prev = buf[0]["open_time"]
        for k in list(buf)[1:]:
            expected = prev + interval_ms
            if k["open_time"] != expected:
                return False, f"gap detected {prev} -> {k['open_time']}"
            prev = k["open_time"]

        return True, "ok"

    async def get_klines(self, symbol: str):
        key = self._get_key(symbol)
        buf = self.klines[key]

        healthy, reason = self._is_kline_buffer_healthy(buf)

        if not healthy:
            log.warning(
                "⚠ kline buffer unhealthy %s reason=%s → trigger resync",
                symbol,
                reason,
            )
            # resync rest and return new klines
            await self.resync_rest(symbol)
            buf = self.klines[key]

        return buf

    async def get_all_signals(self) -> list[dict]:
        """
        Get signals for all symbols currently being monitored.
        Returns a list of signal dicts, one per symbol that has a signal.

        Returns:
            list[dict]: List of signal dicts with format:
                {
                    "symbol": str,
                    "sig": {
                        "time": int,
                        "type": str,
                        "ema_fast": float,
                        "ema_slow": float,
                        "ema_50": float
                    }
                }
        """
        signals_list = []

        if not self.symbols:
            log.warning("No symbols available for signal scanning")
            return signals_list

        for symbol in self.symbols:
            try:
                key = self._get_key(symbol)
                buf = self.klines[key]

                if len(buf) < 2:
                    continue  # Skip if not enough data

                # Get the last closed candle
                closed_candle = buf[-2] if len(buf) >= 2 else None
                if not closed_candle:
                    continue

                # Convert deque to list for scan_signal_service
                kline_list = list(buf)

                # Scan for signals
                signals = await self._scan_signal_service.scan(
                    symbol=symbol,
                    kline_list=kline_list,
                    config=self.config
                )

                # ScanResult
                if signals and signals.state != "NONE":
                    logging.info("🔍 Signals for %s: %s", symbol, signals)
                    signals_list.append(signals)

            except Exception as e:  # noqa: BLE001
                log.error(
                    "Error getting signal for %s: %s",
                    symbol, e, exc_info=True
                )
                continue

        return signals_list
