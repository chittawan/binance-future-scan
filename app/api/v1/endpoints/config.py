from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from app.services.auth_service import require_auth
from app.services.trading_config_service import trading_config_service
from app.services.client_api_service import client_api_service
from app.models.trading_config_model import ClientConfigSymbol
from typing import Optional
import logging

router = APIRouter()


@router.get("/config/clients")
async def get_client_configs(user=Depends(require_auth)):
    """Get all client configs"""
    try:
        config = trading_config_service.get()
        client_configs = config.CLIENT_CONFIGS or []

        return {
            "success": True,
            "clients": [
                {
                    "client_name": client.client_name,
                    "client_api_base_url": client.client_api_base_url
                }
                for client in client_configs
            ]
        }
    except Exception as e:
        logging.error(f"Error in get_client_configs: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@router.post("/config/clients")
async def add_client_config(
    client_config: ClientConfigSymbol,
    user=Depends(require_auth)
):
    """Add a new client config"""
    try:
        logging.info(f"Adding client config: {client_config.client_name}")

        config = trading_config_service.get()
        client_configs = config.CLIENT_CONFIGS or []

        # Check if client already exists
        for existing_client in client_configs:
            if existing_client.client_name == client_config.client_name:
                raise HTTPException(
                    status_code=400,
                    detail=f"Client '{client_config.client_name}' already exists"
                )

        # Add new client config
        client_configs.append(client_config)
        config.CLIENT_CONFIGS = client_configs

        # Save the updated config
        trading_config_service._config = config
        trading_config_service.save_config()

        return {
            "success": True,
            "message": f"Client config '{client_config.client_name}' added successfully",
            "client": {
                "client_name": client_config.client_name,
                "client_api_base_url": client_config.client_api_base_url
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in add_client_config: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@router.delete("/config/clients/{client_name}")
async def delete_client_config(
    client_name: str = Path(..., description="Client name to delete"),
    user=Depends(require_auth)
):
    """Delete a client config"""
    try:
        logging.info(f"Deleting client config: {client_name}")

        config = trading_config_service.get()
        client_configs = config.CLIENT_CONFIGS or []

        # Find and remove client
        original_count = len(client_configs)
        client_configs = [
            client for client in client_configs
            if client.client_name != client_name
        ]

        if len(client_configs) == original_count:
            raise HTTPException(
                status_code=404,
                detail=f"Client '{client_name}' not found"
            )

        config.CLIENT_CONFIGS = client_configs

        # Save the updated config
        trading_config_service._config = config
        trading_config_service.save_config()

        return {
            "success": True,
            "message": f"Client config '{client_name}' deleted successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in delete_client_config: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@router.post("/config/symbol/push/{symbol}")
async def push_symbol_to_client(
    request: Request,
    symbol: str = Path(..., description="Symbol to add (e.g., AERGOUSDT)"),
    client_name: Optional[str] = Query(
        None, description="Optional client name. If not provided, sends to all clients."),
    user=Depends(require_auth)
):
    """
    Send symbol signal to client API.
    If client_name is not provided, sends to all clients.
    Returns summary with status counts for each client.
    """
    try:
        logging.info(f"Pushing symbol {symbol} to client API")

        config = trading_config_service.get()
        client_configs = config.CLIENT_CONFIGS or []

        if not client_configs:
            raise HTTPException(
                status_code=404,
                detail="No client configs found. Please add a client config first."
            )

        # Normalize symbol to uppercase
        symbol = symbol.upper()

        # Get Authorization header from request
        authorization_header = request.headers.get("Authorization", "")
        if not authorization_header:
            raise HTTPException(
                status_code=401,
                detail="Authorization header is required"
            )

        # Validate client_name if provided
        if client_name:
            client_found = any(
                client.client_name == client_name
                for client in client_configs
            )
            if not client_found:
                raise HTTPException(
                    status_code=404,
                    detail=f"Client '{client_name}' not found"
                )

        # Call the service to push symbol to clients
        result = await client_api_service.push_symbol_to_clients(
            client_configs,
            symbol,
            authorization_header,
            client_name
        )

        # Return the summary response
        return result

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in push_symbol_to_client: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )


@router.post("/config/symbol/pop/{symbol}")
async def pop_symbol_from_client(
    request: Request,
    symbol: str = Path(..., description="Symbol to remove (e.g., AERGOUSDT)"),
    client_name: Optional[str] = Query(
        None, description="Optional client name. If not provided, sends to all clients."),
    user=Depends(require_auth)
):
    """
    Send symbol removal signal to client API.
    If client_name is not provided, sends to all clients.
    Returns summary with status counts for each client.
    """
    try:
        logging.info(f"Popping symbol {symbol} from client API")

        config = trading_config_service.get()
        client_configs = config.CLIENT_CONFIGS or []

        if not client_configs:
            raise HTTPException(
                status_code=404,
                detail="No client configs found"
            )

        # Normalize symbol to uppercase
        symbol = symbol.upper()

        # Get Authorization header from request
        authorization_header = request.headers.get("Authorization", "")
        if not authorization_header:
            raise HTTPException(
                status_code=401,
                detail="Authorization header is required"
            )

        # Validate client_name if provided
        if client_name:
            client_found = any(
                client.client_name == client_name
                for client in client_configs
            )
            if not client_found:
                raise HTTPException(
                    status_code=404,
                    detail=f"Client '{client_name}' not found"
                )

        # Call the service to pop symbol from clients
        result = await client_api_service.pop_symbol_from_clients(
            client_configs,
            symbol,
            authorization_header,
            client_name
        )

        # Return the summary response
        return result

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in pop_symbol_from_client: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )
