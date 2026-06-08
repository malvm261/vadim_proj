"""
Coordinator — ядро мастера.

Отвечает за:
- реестр воркеров
- очередь чанков
- агрегацию частичных результатов
- управление жизненным циклом задачи (start / stop / resume)
"""

import textwrap
import threading
import time
from dataclasses import dataclass, field
from typing import Optional


# ── Модели ────────────────────────────────────────────────────────────────────

@dataclass
class Worker:
    worker_id: str
    hostname: str
    registered_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    enabled: bool = True
    status: str = "idle"           # idle | working | done | offline
    current_iteration: int = 0
    partial_sum: float = 0.0       # текущий вклад по активному чанку
    completed_sum: float = 0.0     # подтверждённая сумма по завершённым чанкам
    elapsed: float = 0.0
    task_start: int = 0
    task_end: int = 0


@dataclass
class Task:
    code: str
    total_iterations: int
    chunk_size: int
    chunk_count: int
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    status: str = "pending"        # pending | running | stopped | done


# ── Coordinator ───────────────────────────────────────────────────────────────

class Coordinator:
    def __init__(self):
        self._lock = threading.Lock()
        self.workers: dict[str, Worker] = {}
        self.task: Optional[Task] = None
        self._pending_chunks: list[tuple[int, int]] = []

    # ── Воркеры ───────────────────────────────────────────────────────────────

    def register_worker(self, worker_id: str, hostname: str) -> None:
        with self._lock:
            if worker_id in self.workers:
                # Переподключение — сбрасываем статус, сохраняем историю
                self.workers[worker_id].hostname = hostname
                self.workers[worker_id].status = "idle"
                self.workers[worker_id].last_seen = time.time()
            else:
                self.workers[worker_id] = Worker(
                    worker_id=worker_id,
                    hostname=hostname,
                )

    def touch_worker(self, worker_id: str) -> bool:
        """Обновить время последнего пинга. Вернуть False если воркер неизвестен."""
        with self._lock:
            if worker_id not in self.workers:
                return False
            self.workers[worker_id].last_seen = time.time()
            if self.workers[worker_id].status == "offline":
                self.workers[worker_id].status = "idle"
            return True

    def health_worker(self, worker_id: str, hostname: str | None = None) -> dict:
        """
        Heartbeat для клиентов, которые опрашивают мастер раз в секунду.
        Если воркер свободен и разрешён в UI, сразу возвращаем ему следующий чанк.
        """
        with self._lock:
            if worker_id not in self.workers:
                self.workers[worker_id] = Worker(
                    worker_id=worker_id,
                    hostname=hostname or worker_id,
                )
            else:
                w = self.workers[worker_id]
                if hostname:
                    w.hostname = hostname
                w.last_seen = time.time()
                if w.status == "offline":
                    w.status = "idle"

        chunk = self.pop_chunk_for_worker(worker_id)
        with self._lock:
            return {
                "ok": True,
                "enabled": self.workers[worker_id].enabled,
                "task_status": self.task.status if self.task else None,
                "task": chunk,
            }

    def set_worker_enabled(self, worker_id: str, enabled: bool) -> bool:
        """Разрешить или запретить воркеру получать новые чанки."""
        with self._lock:
            if worker_id not in self.workers:
                return False

            w = self.workers[worker_id]
            w.enabled = enabled
            if not enabled:
                self._requeue_current_chunk(w)
                if w.status != "offline":
                    w.status = "idle"
            return True

    def mark_offline_workers(self, timeout: int) -> None:
        """Пометить воркеров, от которых давно не было heartbeat."""
        now = time.time()
        with self._lock:
            for w in self.workers.values():
                if now - w.last_seen > timeout and w.status != "offline":
                    self._requeue_current_chunk(w)
                    w.status = "offline"

    # ── Управление задачей ────────────────────────────────────────────────────

    def start_task(
        self,
        code: str,
        total_iterations: int,
        chunk_size: int | None = None,
        chunk_count: int | None = None,
    ) -> str:
        """
        Создать новую задачу и нарезать её на чанки.
        Вернуть сообщение об ошибке или пустую строку если всё OK.
        """
        with self._lock:
            if total_iterations < 1:
                return "Количество итераций должно быть больше 0"
            if chunk_count is not None and chunk_count < 1:
                return "Количество чанков должно быть больше 0"
            if chunk_size is not None and chunk_size < 1:
                return "Размер чанка должен быть больше 0"

            if chunk_count is None:
                if chunk_size is None:
                    return "Нужно указать количество чанков или размер чанка"
                chunk_count = (total_iterations + chunk_size - 1) // chunk_size

            chunk_count = min(chunk_count, total_iterations)
            chunks = self._make_chunks(total_iterations, chunk_count)
            effective_chunk_size = max(end - start for start, end in chunks)

            active_workers = [
                w for w in self.workers.values() if w.enabled and w.status != "offline"
            ]
            if not active_workers:
                return "Нет выбранных подключённых воркеров"

            code = textwrap.dedent(code).strip()

            self.task = Task(
                code=code,
                total_iterations=total_iterations,
                chunk_size=effective_chunk_size,
                chunk_count=len(chunks),
                started_at=time.time(),
                status="running",
            )

            self._pending_chunks = chunks

            for w in self.workers.values():
                if w.status != "offline":
                    w.status = "idle"
                    w.current_iteration = 0
                    w.partial_sum = 0.0
                    w.completed_sum = 0.0
                    w.elapsed = 0.0
                    w.task_start = 0
                    w.task_end = 0

            return ""

    def stop_task(self) -> None:
        with self._lock:
            if not self.task:
                return
            for w in self.workers.values():
                if w.status == "working":
                    self._requeue_current_chunk(w)
                    w.status = "idle"
            self.task.status = "stopped"

    def resume_task(self) -> str:
        """Продолжить остановленную задачу с оставшихся чанков."""
        with self._lock:
            if not self.task or self.task.status != "stopped":
                return "Нет остановленной задачи"

            self.task.status = "running"
            self.task.finished_at = None

            for w in self.workers.values():
                if w.enabled and w.status != "offline":
                    w.status = "idle"

            return ""

    # ── Выдача заданий воркерам ───────────────────────────────────────────────

    def pop_chunk_for_worker(self, worker_id: str) -> Optional[dict]:
        """
        Выдать воркеру следующий чанк.
        Вернуть None если задачи нет или очередь пуста.
        """
        with self._lock:
            if worker_id not in self.workers:
                return None
            if not self.task or self.task.status != "running":
                return None
            if not self._pending_chunks:
                return None

            start, end = self._pending_chunks.pop(0)

            w = self.workers[worker_id]
            if not w.enabled or w.status == "offline" or w.status == "working":
                self._pending_chunks.insert(0, (start, end))
                return None

            w.status = "working"
            w.task_start = start
            w.task_end = end
            w.current_iteration = start
            w.last_seen = time.time()

            return {
                "code": self._code_for_worker(self.task),
                "start": start,
                "end": end,
                "total_iterations": self.task.total_iterations,
                "chunk_size": self.task.chunk_size,
                "chunk_count": self.task.chunk_count,
            }

    # ── Приём результатов ─────────────────────────────────────────────────────

    def update_progress(
        self, worker_id: str, iteration: int, partial_sum: float, elapsed: float
    ) -> None:
        with self._lock:
            if worker_id not in self.workers:
                return
            w = self.workers[worker_id]
            if w.status != "working":
                return
            w.current_iteration = iteration
            w.partial_sum = partial_sum
            w.elapsed = elapsed
            w.last_seen = time.time()

    def accept_result(
        self, worker_id: str, partial_sum: float, elapsed: float
    ) -> None:
        with self._lock:
            if worker_id not in self.workers:
                return
            w = self.workers[worker_id]
            if not self.task or self.task.status != "running" or w.status != "working":
                return

            w.status = "done"
            w.completed_sum += partial_sum
            w.partial_sum = 0.0
            w.elapsed = elapsed
            w.current_iteration = w.task_end
            w.last_seen = time.time()
            w.task_start = 0
            w.task_end = 0

            self._check_completion()

    # ── Снимок состояния для API / UI ─────────────────────────────────────────

    def snapshot(self) -> dict:
        with self._lock:
            result = self._aggregate_result()

            task_info = None
            if self.task:
                elapsed = None
                if self.task.started_at:
                    end_ts = self.task.finished_at or time.time()
                    elapsed = round(end_ts - self.task.started_at, 3)

                task_info = {
                    "status": self.task.status,
                    "total_iterations": self.task.total_iterations,
                    "chunk_size": self.task.chunk_size,
                    "chunk_count": self.task.chunk_count,
                    "chunks_remaining": len(self._pending_chunks),
                    "elapsed": elapsed,
                }

            workers_list = [
                {
                    "worker_id": w.worker_id,
                    "hostname": w.hostname,
                    "enabled": w.enabled,
                    "status": w.status,
                    "current_iteration": w.current_iteration,
                    "task_range": [w.task_start, w.task_end] if w.task_end else None,
                    "partial_result": round(w.completed_sum + w.partial_sum, 12)
                    if w.completed_sum or w.partial_sum
                    else None,
                    "elapsed": round(w.elapsed, 3),
                    "last_seen_ago": round(time.time() - w.last_seen, 1),
                }
                for w in self.workers.values()
            ]

            return {
                "task": task_info,
                "workers": workers_list,
                "result": result,
            }

    # ── Приватные хелперы ─────────────────────────────────────────────────────

    @staticmethod
    def _make_chunks(total: int, chunk_count: int) -> list[tuple[int, int]]:
        base_size, remainder = divmod(total, chunk_count)
        chunks = []
        start = 0

        for index in range(chunk_count):
            size = base_size + (1 if index < remainder else 0)
            end = start + size
            chunks.append((start, end))
            start = end

        return chunks

    def _aggregate_result(self) -> Optional[float]:
        """Суммируем вклады всех воркеров. Вызывать под локом."""
        if not self.task:
            return None
        total = sum(w.completed_sum + w.partial_sum for w in self.workers.values())
        return round(total, 12)

    @staticmethod
    def _code_for_worker(task: Task) -> str:
        return (
            f"TOTAL_ITERATIONS = {task.total_iterations}\n"
            f"CHUNK_SIZE = {task.chunk_size}\n"
            f"CHUNK_COUNT = {task.chunk_count}\n\n"
            f"{task.code}"
        )

    def _requeue_current_chunk(self, worker: Worker) -> None:
        """Вернуть незавершённый чанк в очередь. Вызывать под локом."""
        if (
            self.task
            and self.task.status == "running"
            and worker.status == "working"
            and worker.task_end > worker.task_start
        ):
            self._pending_chunks.insert(0, (worker.task_start, worker.task_end))

        worker.partial_sum = 0.0
        worker.current_iteration = 0
        worker.task_start = 0
        worker.task_end = 0

    def _check_completion(self) -> None:
        """Проверить, все ли чанки розданы и обработаны. Вызывать под локом."""
        if not self.task or self.task.status != "running":
            return
        if self._pending_chunks:
            return
        all_done = all(
            w.status in ("done", "idle", "offline")
            for w in self.workers.values()
        )
        if all_done:
            self.task.status = "done"
            self.task.finished_at = time.time()
