import asyncio
import logging
from app.api.router import router
from app.services.signal_service.pool_signal_service import get_pool_signal_service
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# Debug mode
# asyncio.get_event_loop().set_debug(True)


# ตั้งค่า logging ที่นี่เพียงครั้งเดียว
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True  # override การตั้งค่าเดิม
)

app = FastAPI(title="Binance Futures Connector")
# CORS Configuration
# Allow origins for both HTTP and WebSocket connections
# Note: WebSocket connections use the Origin header from the HTTP upgrade request
app.add_middleware(
    CORSMiddleware,
    # Allow production domains and localhost with any port for development
    # Pattern matches:
    # - https://*.codewalk.myds.me (production)
    # - http://localhost:* (development - any port)
    # - http://127.0.0.1:* (development - any port)
    allow_origin_regex=r"(https://.*\.codewalk\.myds\.me|http://localhost:\d+|http://127\.0\.0\.1:\d+)",
    # allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type",
                   "Origin", "X-Requested-With"],
    max_age=86400,  # ⭐ สำคัญมาก
)

app.include_router(router, prefix="/api/v1")


@app.on_event("startup")
async def startup_event():
    from app.services.trading_config_service import trading_config_service
    config = trading_config_service.get()
    _pool_signal_sv = get_pool_signal_service(config)
    await _pool_signal_sv.start()


@app.on_event("shutdown")
async def shutdown_event():
    asyncio.sleep(10)
