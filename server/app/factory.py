"""
Фабрика FastAPI приложения.

Здесь подключаются роутеры, статика и фоновые задачи.
"""

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.api.control import router as control_router
from app.api.workers import router as workers_router
from app.config import settings
from app.core.state import coordinator

# ── Фоновая задача: пометка оффлайн-воркеров ─────────────────────────────────


async def _offline_watcher():
    """Каждые 5 секунд помечает воркеров без heartbeat как offline."""
    while True:
        await asyncio.sleep(5)
        coordinator.mark_offline_workers(timeout=settings.worker_timeout)


# ── Lifespan (заменяет устаревший @app.on_event) ─────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_offline_watcher())
    yield
    task.cancel()


# ── Фабрика ───────────────────────────────────────────────────────────────────


def create_app() -> FastAPI:
    app = FastAPI(
        title="Distributed Compute Master",
        lifespan=lifespan,
    )

    app.include_router(workers_router)
    app.include_router(control_router)

    # Статика (index.html)
    static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
    static_dir = os.path.abspath(static_dir)
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index():
        html_path = os.path.join(static_dir, "index.html")
        with open(html_path, encoding="utf-8") as f:
            return f.read()

    return app
