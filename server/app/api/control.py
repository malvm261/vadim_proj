"""
Эндпоинты для управления из браузера:
  POST /control/start   — запустить задачу
  POST /control/stop    — остановить
  POST /control/resume  — продолжить
  GET  /control/state   — текущее состояние (polling из JS)
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.state import coordinator
from app.config import settings

router = APIRouter(prefix="/control", tags=["control"])


# ── Схемы ─────────────────────────────────────────────────────────────────────

class StartRequest(BaseModel):
    code: str
    total_iterations: int = settings.default_total_iterations
    chunk_count: int | None = settings.default_chunk_count
    chunk_size: int | None = None


class WorkerEnabledRequest(BaseModel):
    enabled: bool


# ── Эндпоинты ─────────────────────────────────────────────────────────────────

@router.post("/start", summary="Запустить задачу")
def start(req: StartRequest):
    error = coordinator.start_task(
        req.code,
        req.total_iterations,
        chunk_size=req.chunk_size,
        chunk_count=req.chunk_count,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)
    return {"ok": True}


@router.post("/stop", summary="Остановить задачу")
def stop():
    coordinator.stop_task()
    return {"ok": True}


@router.post("/resume", summary="Продолжить остановленную задачу")
def resume():
    error = coordinator.resume_task()
    if error:
        raise HTTPException(status_code=400, detail=error)
    return {"ok": True}


@router.get("/state", summary="Текущее состояние системы")
def state():
    return coordinator.snapshot()


@router.post("/workers/{worker_id}/enabled", summary="Включить или выключить воркер")
def set_worker_enabled(worker_id: str, req: WorkerEnabledRequest):
    ok = coordinator.set_worker_enabled(worker_id, req.enabled)
    if not ok:
        raise HTTPException(status_code=404, detail="Неизвестный воркер")
    return {"ok": True}
