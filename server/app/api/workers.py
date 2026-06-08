"""
Эндпоинты для воркеров:
  POST /workers/register   — регистрация
  POST /workers/heartbeat  — пинг (воркер жив)
  GET  /workers/task       — запросить следующий чанк
  POST /workers/progress   — прислать промежуточный результат
  POST /workers/result     — прислать финальный результат чанка
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.core.state import coordinator

router = APIRouter(prefix="/workers", tags=["workers"])


# ── Схемы запросов ────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    worker_id: str
    hostname: str
    group: Optional[str] = None


class WorkerIDRequest(BaseModel):
    worker_id: str


class HealthRequest(BaseModel):
    worker_id: str
    hostname: Optional[str] = None
    group: Optional[str] = None


class ProgressRequest(BaseModel):
    worker_id: str
    iteration: int
    partial_sum: float   # текущий вклад по активному чанку
    elapsed: float


class ResultRequest(BaseModel):
    worker_id: str
    partial_sum: float   # финальный вклад завершённого чанка
    elapsed: float


# ── Эндпоинты ─────────────────────────────────────────────────────────────────

@router.post("/register", summary="Регистрация воркера")
def register(req: RegisterRequest):
    coordinator.register_worker(req.worker_id, req.hostname, req.group or "")
    return {"ok": True}


@router.post("/heartbeat", summary="Воркер сообщает что жив")
def heartbeat(req: WorkerIDRequest):
    ok = coordinator.touch_worker(req.worker_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Неизвестный воркер")
    return {"ok": True}


@router.post("/health", summary="Heartbeat и запрос работы одним вызовом")
def health(req: HealthRequest):
    return coordinator.health_worker(req.worker_id, req.hostname, req.group)


@router.get("/task", summary="Запросить следующий чанк")
def get_task(worker_id: str):
    chunk = coordinator.pop_chunk_for_worker(worker_id)
    # chunk = None означает "заданий пока нет, приходи позже"
    return {"task": chunk}


@router.post("/progress", summary="Промежуточный прогресс")
def report_progress(req: ProgressRequest):
    coordinator.update_progress(
        req.worker_id, req.iteration, req.partial_sum, req.elapsed
    )
    return {"ok": True}


@router.post("/result", summary="Финальный результат чанка")
def report_result(req: ResultRequest):
    coordinator.accept_result(req.worker_id, req.partial_sum, req.elapsed)
    return {"ok": True}
