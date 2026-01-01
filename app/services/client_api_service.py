"""
Service for making API calls to external client services.
"""
import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Any
from app.models.trading_config_model import ClientConfigSymbol

logger = logging.getLogger(__name__)


class ClientApiService:
    """Service for calling external client APIs"""

    @staticmethod
    async def push_symbol(
        client_config: ClientConfigSymbol,
        symbol: str,
        authorization_header: str,
        timeout: int = 10
    ) -> Dict[str, Any]:
        """
        Push a symbol to a client API.

        Args:
            client_config: Client configuration
            symbol: Symbol to push
            authorization_header: Authorization header from the original request
            timeout: Request timeout in seconds

        Returns:
            Dict with status and details
        """
        url = f"{client_config.client_api_base_url.rstrip('/')}/api/v1/config/config/symbol/push/{symbol}"
        headers = {
            "Authorization": authorization_header,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    response_data = await response.json() if response.content_type == "application/json" else {}

                    if response.status == 200:
                        # Check if symbol was already added
                        if response_data.get("status") == "already exists" or "already exists" in str(response_data.get("message", "")).lower():
                            return {
                                "status": "already exists",
                                "symbol": symbol,
                                "client_name": client_config.client_name,
                                "status_code": response.status
                            }
                        else:
                            return {
                                "status": "pushed",
                                "symbol": symbol,
                                "client_name": client_config.client_name,
                                "status_code": response.status,
                                "response": response_data
                            }
                    else:
                        return {
                            "status": "error",
                            "symbol": symbol,
                            "client_name": client_config.client_name,
                            "status_code": response.status,
                            "error": response_data.get("detail") or response_data.get("error") or f"HTTP {response.status}"
                        }
        except aiohttp.ClientError as e:
            logger.error(
                f"Client error pushing {symbol} to {client_config.client_name}: {e}")
            return {
                "status": "error",
                "symbol": symbol,
                "client_name": client_config.client_name,
                "error": str(e)
            }
        except asyncio.TimeoutError:
            logger.error(
                f"Timeout pushing {symbol} to {client_config.client_name}")
            return {
                "status": "error",
                "symbol": symbol,
                "client_name": client_config.client_name,
                "error": "Request timeout"
            }
        except Exception as e:
            logger.error(
                f"Unexpected error pushing {symbol} to {client_config.client_name}: {e}")
            return {
                "status": "error",
                "symbol": symbol,
                "client_name": client_config.client_name,
                "error": str(e)
            }

    @staticmethod
    async def pop_symbol(
        client_config: ClientConfigSymbol,
        symbol: str,
        authorization_header: str,
        timeout: int = 10
    ) -> Dict[str, Any]:
        """
        Pop (remove) a symbol from a client API.

        Args:
            client_config: Client configuration
            symbol: Symbol to remove
            authorization_header: Authorization header from the original request
            timeout: Request timeout in seconds

        Returns:
            Dict with status and details
        """
        url = f"{client_config.client_api_base_url.rstrip('/')}/api/v1/config/config/symbol/pop/{symbol}"
        headers = {
            "Authorization": authorization_header,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    response_data = await response.json() if response.content_type == "application/json" else {}

                    if response.status == 200:
                        return {
                            "status": "popped",
                            "symbol": symbol,
                            "client_name": client_config.client_name,
                            "status_code": response.status,
                            "response": response_data
                        }
                    else:
                        return {
                            "status": "error",
                            "symbol": symbol,
                            "client_name": client_config.client_name,
                            "status_code": response.status,
                            "error": response_data.get("detail") or response_data.get("error") or f"HTTP {response.status}"
                        }
        except aiohttp.ClientError as e:
            logger.error(
                f"Client error popping {symbol} from {client_config.client_name}: {e}")
            return {
                "status": "error",
                "symbol": symbol,
                "client_name": client_config.client_name,
                "error": str(e)
            }
        except asyncio.TimeoutError:
            logger.error(
                f"Timeout popping {symbol} from {client_config.client_name}")
            return {
                "status": "error",
                "symbol": symbol,
                "client_name": client_config.client_name,
                "error": "Request timeout"
            }
        except Exception as e:
            logger.error(
                f"Unexpected error popping {symbol} from {client_config.client_name}: {e}")
            return {
                "status": "error",
                "symbol": symbol,
                "client_name": client_config.client_name,
                "error": str(e)
            }

    @staticmethod
    async def push_symbol_to_clients(
        client_configs: List[ClientConfigSymbol],
        symbol: str,
        authorization_header: str,
        client_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Push symbol to multiple clients.

        Args:
            client_configs: List of client configurations
            symbol: Symbol to push
            authorization_header: Authorization header from the original request
            client_name: Optional specific client name to target

        Returns:
            Summary dict with status counts and results
        """
        # Filter clients if client_name is specified
        targets = [
            client for client in client_configs
            if not client_name or client.client_name == client_name
        ]

        if not targets:
            return {
                "success": False,
                "message": "No clients found" + (f" matching '{client_name}'" if client_name else ""),
                "symbol": symbol,
                "status_counts": {},
                "results": []
            }

        # Call all clients concurrently
        results = await asyncio.gather(*[
            ClientApiService.push_symbol(client, symbol, authorization_header)
            for client in targets
        ])

        # Count statuses
        status_counts = {
            "pushed": 0,
            "already exists": 0,
            "error": 0
        }

        for result in results:
            status = result.get("status", "error")
            if status in status_counts:
                status_counts[status] += 1
            else:
                status_counts["error"] += 1

        # Check if all succeeded (no errors)
        all_success = status_counts["error"] == 0

        return {
            "success": all_success,
            "message": f"Symbol {symbol} processed for {len(targets)} client(s)",
            "symbol": symbol,
            "status_counts": status_counts,
            "results": results
        }

    @staticmethod
    async def pop_symbol_from_clients(
        client_configs: List[ClientConfigSymbol],
        symbol: str,
        authorization_header: str,
        client_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Pop symbol from multiple clients.

        Args:
            client_configs: List of client configurations
            symbol: Symbol to remove
            authorization_header: Authorization header from the original request
            client_name: Optional specific client name to target

        Returns:
            Summary dict with status counts and results
        """
        # Filter clients if client_name is specified
        targets = [
            client for client in client_configs
            if not client_name or client.client_name == client_name
        ]

        if not targets:
            return {
                "success": False,
                "message": "No clients found" + (f" matching '{client_name}'" if client_name else ""),
                "symbol": symbol,
                "status_counts": {},
                "results": []
            }

        # Call all clients concurrently
        results = await asyncio.gather(*[
            ClientApiService.pop_symbol(client, symbol, authorization_header)
            for client in targets
        ])

        # Count statuses
        status_counts = {
            "popped": 0,
            "error": 0
        }

        for result in results:
            status = result.get("status", "error")
            if status in status_counts:
                status_counts[status] += 1
            else:
                status_counts["error"] += 1

        # Check if all succeeded (no errors)
        all_success = status_counts["error"] == 0

        return {
            "success": all_success,
            "message": f"Symbol {symbol} removal processed for {len(targets)} client(s)",
            "symbol": symbol,
            "status_counts": status_counts,
            "results": results
        }


# Singleton instance
client_api_service = ClientApiService()
