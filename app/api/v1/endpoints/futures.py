
from typing import Set, Any
from app.services.auth_service import require_auth
from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from app.services.client_ws_service import client_ws_service
from app.services.signal_service.pool_signal_service import get_pool_signal_service

import logging

# logging.basicConfig(level=logging.INFO)  # ใช้การตั้งค่าจาก main.py แทน

router = APIRouter()

# Store active WebSocket connections from frontend
frontend_connections: Set[WebSocket] = set()


def _normalize_binance_websocket_data(data: Any) -> Any:
    """
    Normalize Binance WebSocket data format to standard format.

    Binance WebSocket uses short field names:
    - Balance: a=asset, wb=wallet_balance, cw=cross_wallet_balance, bc=balance_change
    - Position: s=symbol, pa=position_amt, ep=entry_price, cr=accumulated_realized,
                up=unrealized_profit, mt=margin_type, iw=isolated_wallet, ps=position_side,
                ma=margin_asset, bep=break_even_price

    Converts to standard format:
    - Balance: asset, balance, available_balance
    - Position: symbol, positionAmt, entryPrice, unRealizedProfit, etc.
    """
    if isinstance(data, list):
        return [_normalize_binance_websocket_data(item) for item in data]
    elif isinstance(data, dict):
        normalized = {}
        for key, value in data.items():
            # Normalize balance fields
            if key == 'a':  # asset
                normalized['asset'] = value
            elif key == 'wb':  # wallet balance
                normalized['balance'] = value
                normalized['wallet_balance'] = value
            elif key == 'cw':  # cross wallet balance
                normalized['available_balance'] = value
                normalized['cross_wallet_balance'] = value
            elif key == 'bc':  # balance change
                normalized['balance_change'] = value
            # Normalize position fields
            elif key == 's':  # symbol
                normalized['symbol'] = value
            elif key == 'pa':  # position amount
                normalized['positionAmt'] = value
                normalized['position_amt'] = value
            elif key == 'ep':  # entry price
                normalized['entryPrice'] = value
                normalized['entry_price'] = value
            elif key == 'cr':  # accumulated realized
                normalized['accumulatedRealized'] = value
                normalized['accumulated_realized'] = value
            elif key == 'up':  # unrealized profit
                normalized['unRealizedProfit'] = value
                normalized['unrealized_profit'] = value
            elif key == 'mt':  # margin type
                normalized['marginType'] = value
                normalized['margin_type'] = value
            elif key == 'iw':  # isolated wallet
                normalized['isolatedWallet'] = value
                normalized['isolated_wallet'] = value
            elif key == 'ps':  # position side
                normalized['positionSide'] = value
                normalized['position_side'] = value
            elif key == 'ma':  # margin asset
                normalized['marginAsset'] = value
                normalized['margin_asset'] = value
            elif key == 'bep':  # break even price
                normalized['breakEvenPrice'] = value
                normalized['break_even_price'] = value
            else:
                # Keep original key and recursively normalize value
                normalized[key] = _normalize_binance_websocket_data(value)
        return normalized
    else:
        return data


def _convert_to_json_serializable(obj: Any) -> Any:
    """
    Convert SDK response objects and other non-JSON-serializable objects to JSON-serializable format.

    Handles:
    - Already JSON-serializable types (dict, list, str, int, float, bool, None)
    - SDK response objects with .data() method
    - Pydantic models with .model_dump() or .dict() methods
    - Objects with __dict__ attribute
    - Lists/iterables (recursively converts items)
    """
    # Already JSON-serializable types
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj

    # Handle dictionaries - recursively convert values
    if isinstance(obj, dict):
        return {key: _convert_to_json_serializable(value) for key, value in obj.items()}

    # Handle lists/tuples - recursively convert items
    if isinstance(obj, (list, tuple)):
        return [_convert_to_json_serializable(item) for item in obj]

    # Handle SDK response objects with .data() method
    if hasattr(obj, 'data'):
        try:
            data = obj.data() if callable(obj.data) else obj.data
            return _convert_to_json_serializable(data)
        except Exception:  # pylint: disable=broad-except
            pass

    # Handle Pydantic models
    if hasattr(obj, 'model_dump'):
        try:
            return _convert_to_json_serializable(obj.model_dump())
        except Exception:  # pylint: disable=broad-except
            pass

    if hasattr(obj, 'dict'):
        try:
            if callable(obj.dict):
                return _convert_to_json_serializable(obj.dict())
            else:
                return _convert_to_json_serializable(obj.dict)
        except Exception:  # pylint: disable=broad-except
            pass

    # Handle objects with __dict__ attribute
    if hasattr(obj, '__dict__'):
        try:
            obj_dict = {}
            for key, value in obj.__dict__.items():
                # Skip private attributes and methods
                if not key.startswith('_') or key in ['_data', '_response']:
                    obj_dict[key] = _convert_to_json_serializable(value)
            return obj_dict
        except Exception:  # pylint: disable=broad-except
            pass

    # Try to extract common attributes from SDK response objects
    if hasattr(obj, '__class__'):
        result = {}
        # Common attributes in Binance SDK responses
        common_attrs = ['asset', 'balance', 'available_balance', 'symbol', 'positionAmt',
                        'entryPrice', 'markPrice', 'unRealizedProfit', 'liquidationPrice',
                        'leverage', 'maxNotionalValue', 'marginType', 'isolatedMargin',
                        'updateTime', 'notional', 'isolatedWallet']

        for attr in common_attrs:
            if hasattr(obj, attr):
                try:
                    value = getattr(obj, attr)
                    result[attr] = _convert_to_json_serializable(value)
                except Exception:  # pylint: disable=broad-except
                    pass

        if result:
            return result

    # Last resort: convert to string
    return str(obj)


@router.post("/websocket/account/subscribe")
async def subscribe_account_websocket(user=Depends(require_auth)):
    """
    Subscribe to account WebSocket stream for real-time balance and position updates.

    This will start receiving:
    - Balance updates when account balance changes
    - Position updates when positions change
    - Order updates when orders are filled/cancelled

    Note: The WebSocket connection will automatically refresh the listen key every 30 minutes.
    """
    try:
        logging.info("Subscribing to account WebSocket stream")

        # Define callbacks for balance and position updates
        async def on_signal(signal):
            """Handle signal updates"""
            logging.info("Signal updated: %s", signal)
            # Update cache is handled automatically in the service

        await client_ws_service.subscribe(
            scan_signal_callback=on_signal,
        )

        return {
            "status": "subscribed",
            "message": "Successfully subscribed to account WebSocket stream"
        }
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Exception in subscribe_account_websocket: %s", e)
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/websocket/account/unsubscribe")
async def unsubscribe_account_websocket(user=Depends(require_auth)):
    """
    Unsubscribe from account WebSocket stream.
    """
    try:
        logging.info("Unsubscribing from account WebSocket stream")
        await client_ws_service.unsubscribe()
        return {
            "status": "unsubscribed",
            "message": "Successfully unsubscribed from account WebSocket stream"
        }
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Exception in unsubscribe_account_websocket: %s", e)
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/websocket/account/status")
async def get_client_websocket_status(user=Depends(require_auth)):
    """
    Get the status of the account WebSocket connection.
    """
    try:
        status = {
            "connected": client_ws_service._connection_initialized,  # pylint: disable=protected-access
            "has_listen_key": client_ws_service._listen_key is not None,  # pylint: disable=protected-access
            "has_websocket": client_ws_service._websocket is not None,  # pylint: disable=protected-access
        }
        return status
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Exception in get_client_websocket_status: %s", e)
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.websocket("/websocket/client")
async def websocket_client_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for frontend to receive real-time scan signal updates.

    This endpoint:
    1. Accepts WebSocket connections from frontend
    2. Uses polling mechanism (Binance WebSocket is blocked)
    3. Forwards scan signal updates from polling to frontend

    Authentication: Token should be passed as query parameter: ?token=YOUR_TOKEN

    Note: CORS is handled at the FastAPI middleware level. WebSocket connections
    use the Origin header from the HTTP upgrade request for CORS validation.
    """
    # IMPORTANT: Accept connection FIRST before any other operations
    # This prevents code 1006 (Abnormal Closure) errors
    try:
        logging.info("========== WebSocket connection attempt ==========")
        client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "Unknown"
        logging.info("Client: %s", client_info)
        logging.info("URL: %s", websocket.url)
        logging.info("Query params: %s", websocket.query_params)

        # Try to get headers (may not be available before accept)
        # WebSocket CORS is checked at HTTP upgrade level by FastAPI CORS middleware
        try:
            headers_dict = dict(websocket.headers) if hasattr(
                websocket, 'headers') else {}
            # logging.info("Headers: %s", headers_dict)
            origin = headers_dict.get("origin", "N/A")
            logging.info("Origin: %s", origin)
            # Log CORS-related headers
            # logging.info("Sec-WebSocket-Key: %s", headers_dict.get("sec-websocket-key", "N/A")[:20] + "..." if headers_dict.get("sec-websocket-key") else "N/A")
            # logging.info("Sec-WebSocket-Version: %s", headers_dict.get("sec-websocket-version", "N/A"))
        except Exception:  # pylint: disable=broad-except
            logging.warning("Could not read headers before accept")

        # logging.info("Note: Using polling mechanism (Binance WebSocket is blocked)")

        # IMPORTANT: For WebSocket, CORS is handled at the HTTP upgrade level
        # FastAPI CORS middleware should handle this, but we accept first to prevent code 1006
        # Accept connection FIRST to prevent code 1006 errors
        # This must be done before any other operations
        logging.info("Accepting WebSocket connection...")
        try:
            await websocket.accept()
            logging.info("✅ WebSocket connection accepted")
        except Exception as accept_error:  # pylint: disable=broad-except
            logging.error(
                "❌ Failed to accept WebSocket connection: %s", accept_error, exc_info=True)
            # Connection failed - this will result in code 1006 on client side
            return

        # Now we can safely read headers and query params
        logging.info("========== WebSocket connection established ==========")
        logging.info("Client: %s", client_info)
        logging.info("Query params: %s", websocket.query_params)

        # Get token from query parameters (after accept)
        token = websocket.query_params.get("token")
        if not token:
            logging.error("❌ Missing authentication token")
            try:
                await websocket.close(code=1008, reason="Missing authentication token")
            except Exception as e:  # pylint: disable=broad-except
                logging.error("Error closing WebSocket (missing token): %s", e)
            return

        logging.info("Token received (length: %d)", len(token))

        # Add to connections set (after accept)
        frontend_connections.add(websocket)
        logging.info("Frontend WebSocket added to connections. Total: %d", len(
            frontend_connections))

        # Verify token
        try:
            from app.services.auth_service import verify_jwt
            from fastapi import HTTPException
            logging.info("Verifying JWT token...")
            try:
                payload = verify_jwt(token)
                if not payload or "sub" not in payload:
                    logging.error(
                        "❌ Invalid authentication token: payload missing or invalid")
                    try:
                        await websocket.close(code=1008, reason="Invalid authentication token")
                    except Exception as e:  # pylint: disable=broad-except
                        logging.error(
                            "Error closing WebSocket (invalid token): %s", e)
                    frontend_connections.discard(websocket)
                    return
                logging.info(
                    "✅ Token verified successfully. User: %s", payload.get("sub"))
            except HTTPException as http_error:
                # Handle HTTPException from verify_jwt (token expired, invalid, etc.)
                error_detail = str(http_error.detail) if hasattr(
                    http_error, 'detail') else "Authentication failed"
                logging.error("❌ JWT verification failed: %s (status: %s)",
                              error_detail, http_error.status_code)
                try:
                    await websocket.close(code=1008, reason=error_detail)
                except Exception as close_error:  # pylint: disable=broad-except
                    logging.error(
                        "Error closing WebSocket (auth failed): %s", close_error)
                frontend_connections.discard(websocket)
                return
        except Exception as e:  # pylint: disable=broad-except
            logging.error("❌ Error verifying WebSocket token: %s",
                          e, exc_info=True)
            error_message = str(e) if e else "Authentication failed"
            try:
                await websocket.close(code=1008, reason=error_message)
            except Exception as close_error:  # pylint: disable=broad-except
                logging.error(
                    "Error closing WebSocket (auth failed): %s", close_error)
            frontend_connections.discard(websocket)
            return

        # Define callbacks for this specific frontend connection
        async def scan_signal_callback(signals):
            """Callback for scan signal updates"""
            await _broadcast_to_frontend("scan_signal", signals)

        try:
            # Subscribe callbacks (polling will start automatically)
            await client_ws_service.subscribe(
                scan_signal_callback=scan_signal_callback,
            )

            # Send initial data immediately after connection
            try:
                pool_signal_service = get_pool_signal_service()
                signals_list = await pool_signal_service.get_all_signals()

                # Send signals list to frontend in the same format as _broadcast_to_frontend
                if signals_list:
                    try:
                        # Convert and normalize data like _broadcast_to_frontend does
                        data_serializable = _convert_to_json_serializable(
                            signals_list)
                        data_normalized = _normalize_binance_websocket_data(
                            data_serializable)

                        # Send in the same format as _broadcast_to_frontend
                        message = {
                            "type": "scan_signal",
                            "data": data_normalized
                        }
                        await websocket.send_json(message)
                    except (WebSocketDisconnect, RuntimeError, ConnectionError) as send_error:
                        logging.warning(
                            "Connection closed while sending initial signals: %s", send_error)
                        frontend_connections.discard(websocket)
                        raise  # Re-raise to exit the try block
                    except Exception as send_error:  # pylint: disable=broad-except
                        logging.error(
                            "Error sending initial signals: %s", send_error, exc_info=True)
                        # Check if it's a connection error
                        if "not connected" in str(send_error).lower() or "disconnected" in str(send_error).lower():
                            frontend_connections.discard(websocket)
                            raise
            except (WebSocketDisconnect, RuntimeError, ConnectionError) as e:
                logging.warning(
                    "Connection closed during initial data send: %s", e)
                frontend_connections.discard(websocket)
                return  # Exit the function, connection is closed
            except Exception as e:  # pylint: disable=broad-except
                logging.error("Error sending initial data: %s",
                              e, exc_info=True)
                # Check if it's a connection error
                if "not connected" in str(e).lower() or "disconnected" in str(e).lower():
                    frontend_connections.discard(websocket)
                    return
                # Don't close connection for other errors, continue with polling

            # Keep connection alive and handle incoming messages
            while True:
                try:
                    # Check if connection is still in the set before receiving
                    if websocket not in frontend_connections:
                        logging.info(
                            "WebSocket removed from connections, exiting receive loop")
                        break

                    data = await websocket.receive_text()
                    if data == "ping":
                        try:
                            await websocket.send_text("pong")
                        except (WebSocketDisconnect, RuntimeError, ConnectionError) as send_error:
                            logging.warning(
                                "Connection closed while sending pong: %s", send_error)
                            break
                        except Exception as send_error:  # pylint: disable=broad-except
                            if "not connected" in str(send_error).lower() or "disconnected" in str(send_error).lower():
                                logging.warning(
                                    "Connection not connected while sending pong: %s", send_error)
                                break
                            logging.error("Error sending pong: %s", send_error)
                except WebSocketDisconnect:
                    logging.info("WebSocket disconnected normally")
                    break
                except RuntimeError as runtime_error:
                    # Handle "WebSocket is not connected" errors
                    if "not connected" in str(runtime_error).lower():
                        logging.warning(
                            "WebSocket not connected: %s", runtime_error)
                        break
                    logging.error(
                        "RuntimeError in WebSocket connection: %s", runtime_error, exc_info=True)
                    break
                except ConnectionError as conn_error:
                    logging.warning(
                        "Connection error in WebSocket: %s", conn_error)
                    break
                except Exception as e:  # pylint: disable=broad-except
                    # Check if it's a connection-related error
                    error_str = str(e).lower()
                    if "not connected" in error_str or "disconnected" in error_str or "connection closed" in error_str:
                        logging.warning("Connection closed: %s", e)
                        break
                    logging.error(
                        "Error in WebSocket connection: %s", e, exc_info=True)
                    break
        except Exception as e:  # pylint: disable=broad-except
            logging.error("Error in WebSocket endpoint: %s", e, exc_info=True)
        finally:
            frontend_connections.discard(websocket)
            logging.info("Frontend WebSocket disconnected. Total connections: %d", len(
                frontend_connections))

            # Remove callbacks for this connection
            try:
                await client_ws_service.remove_callback(scan_signal_callback)
            except Exception as e:  # pylint: disable=broad-except
                logging.error("Error removing callbacks: %s", e, exc_info=True)
    except Exception as e:  # pylint: disable=broad-except
        logging.error(
            "❌ Error accepting WebSocket connection: %s", e, exc_info=True)
        logging.error("❌ Error type: %s", type(e).__name__)
        # Connection was not accepted, so we can't close it properly
        # This will result in code 1006 (Abnormal Closure) on the client side
        return


async def _broadcast_to_frontend(message_type: str, data: Any):
    """Broadcast message to all connected frontend WebSocket clients"""
    if not frontend_connections:
        return

    # Convert data to JSON-serializable format
    try:
        data_serializable = _convert_to_json_serializable(data)
        data_normalized = _normalize_binance_websocket_data(data_serializable)
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Failed to convert/normalize data: %s", e, exc_info=True)
        return

    message = {
        "type": message_type,
        "data": data_normalized
    }

    disconnected = set()
    # Create a copy of the set to avoid modification during iteration
    connections_copy = set(frontend_connections)
    for connection in connections_copy:
        try:
            # Check if connection is still in the set (might have been removed)
            if connection not in frontend_connections:
                continue
            await connection.send_json(message)
        except (WebSocketDisconnect, RuntimeError, ConnectionError) as e:
            # Connection closed or not connected
            error_str = str(e).lower()
            if "not connected" in error_str or "disconnected" in error_str:
                logging.debug(" while broadcasting %s: %s", message_type, e)
            else:
                logging.warning(
                    "Connection error while broadcasting %s: %s", message_type, e)
            disconnected.add(connection)
        except Exception as e:  # pylint: disable=broad-except
            # Check if it's a connection-related error
            error_str = str(e).lower()
            if "not connected" in error_str or "disconnected" in error_str or "connection closed" in error_str:
                logging.debug(
                    "Connection closed while broadcasting %s: %s", message_type, e)
            else:
                logging.error(
                    "Failed to send %s message to frontend: %s", message_type, e)
            disconnected.add(connection)

    # Remove disconnected connections
    for conn in disconnected:
        frontend_connections.discard(conn)
        logging.debug("Removed disconnected connection from broadcast list. Remaining: %d", len(
            frontend_connections))
