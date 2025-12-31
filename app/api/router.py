from asyncio import futures
from fastapi import APIRouter
from app.api.v1.endpoints import auth, futures, signal

router = APIRouter()

router.include_router(auth.router, prefix="/auth", tags=["auth"])
router.include_router(futures.router, prefix="/futures", tags=["futures"])
router.include_router(signal.router, prefix="/signals", tags=["Signals"])
