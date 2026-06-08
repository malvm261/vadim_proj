import argparse
import json
import math
import multiprocessing
import os
import socket
import time
import uuid
from dataclasses import dataclass, replace
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class WorkerConfig:
    server_url: str
    worker_id: str
    hostname: str
    group: str          # пустая строка = одиночный воркер; иначе — базовый hostname машины
    poll_interval: float
    progress_step: int
    request_timeout: float
    processes: int


class MasterClient:
    """
    HTTP-клиент мастера. Сетевые сбои на длинной задаче не должны стоить
    воркеру всего посчитанного чанка, поэтому запросы повторяются с
    нарастающей паузой перед тем, как сдаться.
    """

    def __init__(
        self,
        server_url: str,
        timeout: float,
        retries: int = 5,
        retry_backoff: float = 1.0,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.retry_backoff = retry_backoff

    def register(self, worker_id: str, hostname: str, group: str = "") -> None:
        self._post("/workers/register", {"worker_id": worker_id, "hostname": hostname, "group": group})

    def health(self, worker_id: str, hostname: str, group: str = "") -> dict[str, Any]:
        return self._post("/workers/health", {"worker_id": worker_id, "hostname": hostname, "group": group})

    def progress(
        self,
        worker_id: str,
        iteration: int,
        partial_sum: float,
        elapsed: float,
    ) -> None:
        self._post(
            "/workers/progress",
            {
                "worker_id": worker_id,
                "iteration": iteration,
                "partial_sum": partial_sum,
                "elapsed": elapsed,
            },
        )

    def result(self, worker_id: str, partial_sum: float, elapsed: float) -> None:
        self._post(
            "/workers/result",
            {"worker_id": worker_id, "partial_sum": partial_sum, "elapsed": elapsed},
        )

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = Request(
            f"{self.server_url}{path}",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        return self._send(req)

    def _send(self, req: Request) -> dict[str, Any]:
        delay = self.retry_backoff
        last_error: Exception | None = None

        for attempt in range(self.retries):
            try:
                with urlopen(req, timeout=self.timeout) as response:
                    body = response.read()
                if not body:
                    return {}
                return json.loads(body.decode("utf-8"))
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                error = RuntimeError(f"master returned HTTP {exc.code}: {detail}")
                if 400 <= exc.code < 500:
                    # Client errors (bad payload, unknown worker) won't heal on retry
                    raise error from exc
                last_error = error
            except URLError as exc:
                last_error = RuntimeError(f"cannot reach master: {exc.reason}")

            if attempt < self.retries - 1:
                time.sleep(delay)
                delay *= 2

        assert last_error is not None
        raise last_error


def build_compute(code: str):
    safe_builtins = {
        "abs": abs,
        "bool": bool,
        "enumerate": enumerate,
        "float": float,
        "int": int,
        "len": len,
        "max": max,
        "min": min,
        "pow": pow,
        "range": range,
        "round": round,
        "sum": sum,
    }
    namespace: dict[str, Any] = {"__builtins__": safe_builtins, "math": math}
    exec(code, namespace)

    compute = namespace.get("compute")
    if not callable(compute):
        raise ValueError("task code must define compute(start, end)")
    return compute


def run_task(client: MasterClient, config: WorkerConfig, task: dict[str, Any]) -> None:
    start = int(task["start"])
    end = int(task["end"])
    compute = build_compute(str(task["code"]))
    progress_step = max(1, config.progress_step)
    total = 0.0
    cursor = start
    started_at = time.monotonic()

    print(f"[{config.worker_id}] task {start}:{end}", flush=True)
    while cursor < end:
        next_cursor = min(cursor + progress_step, end)
        total += float(compute(cursor, next_cursor))
        cursor = next_cursor
        elapsed = time.monotonic() - started_at
        try:
            client.progress(config.worker_id, cursor, total, elapsed)
        except Exception as exc:
            # Промежуточный прогресс — best effort: сетевая заминка не должна
            # стоить воркеру уже посчитанной части чанка.
            print(f"[{config.worker_id}] progress report failed: {exc}", flush=True)

    elapsed = time.monotonic() - started_at
    client.result(config.worker_id, total, elapsed)
    print(
        f"[{config.worker_id}] done {start}:{end}, partial_sum={total}, elapsed={elapsed:.3f}s",
        flush=True,
    )


def run(config: WorkerConfig) -> None:
    client = MasterClient(config.server_url, config.request_timeout)

    while True:
        try:
            client.register(config.worker_id, config.hostname, config.group)
            break
        except KeyboardInterrupt:
            return
        except Exception as exc:
            print(
                f"[{config.worker_id}] cannot reach master to register: {exc}, "
                f"retrying in {config.poll_interval:.1f}s",
                flush=True,
            )
            time.sleep(config.poll_interval)

    print(
        f"[{config.worker_id}] connected to {config.server_url} as {config.hostname}",
        flush=True,
    )

    while True:
        try:
            state = client.health(config.worker_id, config.hostname, config.group)
            if not state.get("enabled", True):
                time.sleep(config.poll_interval)
                continue

            task = state.get("task")
            if task:
                run_task(client, config, task)
            else:
                time.sleep(config.poll_interval)
        except KeyboardInterrupt:
            print(f"\n[{config.worker_id}] stopped", flush=True)
            return
        except Exception as exc:
            print(f"[{config.worker_id}] error: {exc}", flush=True)
            time.sleep(config.poll_interval)


def run_pool(config: WorkerConfig) -> None:
    """
    Запустить несколько процессов-воркеров на одной машине — по одному на ядро
    CPU по умолчанию. Каждое ядро считает свой чанк независимо, поэтому общая
    скорость расчёта на машине растёт почти линейно с числом процессов.
    """
    workers = []
    for index in range(config.processes):
        worker_config = replace(
            config,
            worker_id=f"{config.worker_id}-{index}",
            hostname=f"{config.hostname} #{index}",
            group=config.hostname,   # базовый hostname как ключ группы в UI
        )
        process = multiprocessing.Process(target=run, args=(worker_config,), daemon=False)
        process.start()
        workers.append(process)

    print(
        f"[{config.worker_id}] launched {config.processes} worker processes "
        f"(host has {os.cpu_count()} CPUs)",
        flush=True,
    )

    try:
        for process in workers:
            process.join()
    except KeyboardInterrupt:
        print(f"\n[{config.worker_id}] stopping {len(workers)} worker processes...", flush=True)
        for process in workers:
            process.terminate()
        for process in workers:
            process.join()


def parse_args() -> WorkerConfig:
    parser = argparse.ArgumentParser(description="Worker for Pi Distributed Master")
    parser.add_argument("--server-url", default="http://127.0.0.1:8000")
    parser.add_argument("--worker-id", default=f"worker-{uuid.uuid4().hex[:8]}")
    parser.add_argument("--hostname", default=socket.gethostname())
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--progress-step", type=int, default=10_000)
    parser.add_argument("--request-timeout", type=float, default=5.0)
    parser.add_argument(
        "--processes",
        "-p",
        type=int,
        default=os.cpu_count() or 1,
        help=(
            "Сколько процессов-воркеров запустить на этой машине "
            "(по умолчанию — число ядер CPU; 1 — старое однопроцессное поведение)"
        ),
    )
    args = parser.parse_args()

    return WorkerConfig(
        server_url=args.server_url,
        worker_id=args.worker_id,
        hostname=args.hostname,
        group="",
        poll_interval=args.poll_interval,
        progress_step=args.progress_step,
        request_timeout=args.request_timeout,
        processes=max(1, args.processes),
    )


def main() -> None:
    config = parse_args()
    if config.processes <= 1:
        run(config)
    else:
        run_pool(config)


if __name__ == "__main__":
    main()
