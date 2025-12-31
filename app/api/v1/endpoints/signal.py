import asyncio
from datetime import datetime
from app.models.signal_request import SignalRequest
from app.models.trading_config_model import TradingConfigModel
from app.services.auth_service import require_auth
from app.services.signal_service.candle_service import CandleService
from app.services.signal_service.pool_signal_service import get_pool_signal_service

from fastapi import APIRouter
from fastapi.params import Depends
from fastapi.responses import JSONResponse
import logging

# logging.basicConfig(level=logging.INFO)  # ใช้การตั้งค่าจาก main.py แทน

router = APIRouter()
candle_service = CandleService()


@router.post("/candles")
async def get_price_signales(req: SignalRequest, user=Depends(require_auth)):
    """Get price signales data"""
    try:
        logging.info("Received get_price_signales")
        config = TradingConfigModel(
            SIGNAL_SYMBOL=req.symbol,
            USE_LONG=True,
            USE_SHORT=True,
            SIGNAL_INTERVAL=req.interval,
        )
        _pool_signal_sv = get_pool_signal_service()
        klines = await _pool_signal_sv.get_klines(req.symbol)
        # logging.info("klines count: %s", len(klines))

        req = await candle_service.cal_signal(klines, config, req.isDebug)

        if isinstance(req, dict) and req.get("error"):
            logging.error(
                "Error in get_price_signales response: %s", req.get('error'))
            return JSONResponse(status_code=500, content=req)
        return req
    except Exception as e:
        logging.error("Exception in get_price_signales endpoint: %s", e)
        return JSONResponse(status_code=500, content=str(e))
