"""
WebSocket Service for Account Data (Balance and Position)
Receives real-time account updates from Binance User Data Stream
"""
import logging
import asyncio
from typing import Dict, Optional, Callable, Any
# WebSocket disabled - import commented out
# import websockets
from app.services.signal_service.pool_signal_service import get_pool_signal_service

log = logging.getLogger(__name__)

pool_signal_service = None


def get_pool_signal_service_instance():
    """Get or create pool signal service"""
    global pool_signal_service
    if pool_signal_service is None:
        pool_signal_service = get_pool_signal_service()
    return pool_signal_service


class ClientWebSocketService:
    """
    Service to manage WebSocket connection for user account data.
    Receives real-time updates for balance and position changes.
    """

    def __init__(self):
        self._stream_handle: Optional[Any] = None
        # WebSocket disabled - type hint commented out
        # self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._websocket: Optional[Any] = None
        self._listen_key: Optional[str] = None
        self._callbacks: Dict[str, list[Callable]] = {
            "scan_signal": [],
        }
        self._lock = asyncio.Lock()
        self._connection_initialized = False
        self._keepalive_task: Optional[asyncio.Task] = None
        self._websocket_task: Optional[asyncio.Task] = None
        # 30 minutes (Binance requires refresh every 30-60 min)
        self._keepalive_interval = 1800

        # Polling and debounce mechanism
        self._polling_task: Optional[asyncio.Task] = None
        self._polling_interval = 5.0  # Poll every 5 seconds when no trigger
        self._debounce_delay = 3.0  # 3 seconds debounce after trigger
        self._debounce_task: Optional[asyncio.Task] = None
        self._last_pull_time = 0.0
        self._trigger_pending = False

        # Reconnection mechanism
        self._reconnect_task: Optional[asyncio.Task] = None
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 10
        self._reconnect_delay = 5.0  # Start with 5 seconds
        self._should_reconnect = True

    async def subscribe(
        self,
        scan_signal_callback: Optional[Callable] = None,
    ):
        """
        Subscribe to user data stream for scan signal updates.
        Currently only uses polling (WebSocket from Binance is blocked).

        Args:
            scan_signal_callback: Optional callback for scan signal updates
        """
        # await self._ensure_connection()  # BLOCKED: Binance WebSocket

        async with self._lock:
            # Add callbacks (prevent duplicates by checking if already exists)
            if scan_signal_callback and scan_signal_callback not in self._callbacks["scan_signal"]:
                self._callbacks["scan_signal"].append(scan_signal_callback)

            # Mark as initialized (for polling)
            if not self._connection_initialized:
                self._connection_initialized = True

            # Start polling task if not already running (only use polling now)
            has_callbacks = (len(self._callbacks["scan_signal"]) > 0)

            if has_callbacks and (self._polling_task is None or self._polling_task.done()):
                self._polling_task = asyncio.create_task(self._polling_loop())

    async def _polling_loop(self):
        """Background task to poll data every 60 seconds when no trigger"""
        # Wait a bit for callbacks to be registered if polling started early
        if (len(self._callbacks["scan_signal"]) == 0):
            await asyncio.sleep(2.0)

        try:
            while self._connection_initialized:
                try:
                    interval = 60.0
                    await asyncio.sleep(interval)

                    current_time = asyncio.get_event_loop().time()
                    time_since_last_pull = current_time - self._last_pull_time

                    has_callbacks = (len(self._callbacks["scan_signal"]) > 0)

                    if not has_callbacks:
                        await asyncio.sleep(1.0)
                        continue

                    if not self._trigger_pending and time_since_last_pull >= interval:
                        await self._scan_signal_pull_and_send_all_data()

                except asyncio.CancelledError:
                    break
                except Exception as e:  # pylint: disable=broad-except
                    log.error("Error in polling loop: %s", e, exc_info=True)  # noqa: G201
        except asyncio.CancelledError:
            pass
        except Exception as e:  # pylint: disable=broad-except
            log.error("Fatal error in polling loop: %s", e, exc_info=True)  # noqa: G201

    async def _scan_signal_pull_and_send_all_data(self):
        """Pull scan signals for all symbols and send to frontend"""
        try:
            # Get scan signals for all symbols
            try:
                pool_signal_svc = get_pool_signal_service_instance()
                signals_list = await pool_signal_svc.get_all_signals()
            except Exception as e:  # pylint: disable=broad-except
                log.error("Error getting signals: %s", e, exc_info=True)  # noqa: G201
                signals_list = []

            # Send signals to all callbacks
            if signals_list:
                for callback in self._callbacks["scan_signal"]:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(signals_list)
                        else:
                            callback(signals_list)
                    except Exception as e:  # pylint: disable=broad-except
                        log.error("Error in scan_signal callback: %s", e, exc_info=True)  # noqa: G201

            # Update last pull time
            self._last_pull_time = asyncio.get_event_loop().time()

        except Exception as e:  # pylint: disable=broad-except
            log.error("Error pulling and sending all data: %s", e, exc_info=True)  # noqa: G201

    async def remove_callback(self, callback: Callable):
        """Remove a specific callback from all callback lists"""
        async with self._lock:
            if callback in self._callbacks["scan_signal"]:
                self._callbacks["scan_signal"].remove(callback)

    async def unsubscribe(self):
        """Unsubscribe from user data stream"""
        async with self._lock:
            # Stop reconnection
            self._should_reconnect = False

            # Cancel reconnect task
            if self._reconnect_task:
                self._reconnect_task.cancel()
                try:
                    await self._reconnect_task
                except asyncio.CancelledError:
                    pass
                self._reconnect_task = None

            # Cancel polling task
            if self._polling_task:
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except asyncio.CancelledError:
                    pass
                self._polling_task = None

            # Cancel debounce task
            if self._debounce_task:
                self._debounce_task.cancel()
                try:
                    await self._debounce_task
                except asyncio.CancelledError:
                    pass
                self._debounce_task = None

            if self._websocket:
                try:
                    await self._websocket.close()
                    self._websocket = None
                except Exception as e:  # pylint: disable=broad-except
                    log.error("Error unsubscribing: %s", e)  # noqa: G201

            # Cancel WebSocket receive task
            if self._websocket_task:
                self._websocket_task.cancel()
                try:
                    await self._websocket_task
                except asyncio.CancelledError:
                    pass
                self._websocket_task = None

            # Clear callbacks
            self._callbacks = {
                "scan_signal": [],
            }

    async def close(self):
        """Close WebSocket connection and delete listen key"""
        async with self._lock:
            # Stop reconnection
            self._should_reconnect = False

            # Cancel reconnect task
            if self._reconnect_task:
                self._reconnect_task.cancel()
                try:
                    await self._reconnect_task
                except asyncio.CancelledError:
                    pass
                self._reconnect_task = None

            # Cancel polling task
            if self._polling_task:
                self._polling_task.cancel()
                try:
                    await self._polling_task
                except asyncio.CancelledError:
                    pass
                self._polling_task = None

            # Cancel debounce task
            if self._debounce_task:
                self._debounce_task.cancel()
                try:
                    await self._debounce_task
                except asyncio.CancelledError:
                    pass
                self._debounce_task = None

            # Cancel keepalive task
            if self._keepalive_task:
                self._keepalive_task.cancel()
                try:
                    await self._keepalive_task
                except asyncio.CancelledError:
                    pass
                self._keepalive_task = None

            # Cancel WebSocket receive task
            if self._websocket_task:
                self._websocket_task.cancel()
                try:
                    await self._websocket_task
                except asyncio.CancelledError:
                    pass
                self._websocket_task = None

            # Close WebSocket connection
            if self._websocket:
                try:
                    await self._websocket.close()
                except Exception as e:  # pylint: disable=broad-except
                    log.error("Error closing WebSocket connection: %s", e)  # noqa: G201
                self._websocket = None

            # Delete listen key
            if self._listen_key:
                try:
                    client = self._get_rest_client()
                    client.rest_api.close_user_data_stream(
                        listen_key=self._listen_key)
                except Exception as e:  # pylint: disable=broad-except
                    log.error("Error deleting listen key: %s", e)  # noqa: G201
                self._listen_key = None

            self._connection_initialized = False
            self._callbacks = {
                "scan_signal": [],
            }


# Global instance
client_ws_service = ClientWebSocketService()
