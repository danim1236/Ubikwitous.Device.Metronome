"""Global chunk scheduler shared by all camera streams."""

import logging
import threading
import time
from typing import Callable, List


RotateCallback = Callable[[int], None]


class ChunkScheduler:
    """Broadcasts synchronized chunk rotation timestamps to all consumers."""

    def __init__(self, chunk_duration_ms: int, logger: logging.Logger) -> None:
        self._chunk_duration_ms = chunk_duration_ms
        self._logger = logger
        self._callbacks: List[RotateCallback] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="chunk-scheduler", daemon=True)

    def register(self, callback: RotateCallback) -> None:
        with self._lock:
            self._callbacks.append(callback)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=5)

    def current_chunk_timestamp(self) -> int:
        now_ms = int(time.time() * 1000)
        return (now_ms // self._chunk_duration_ms) * self._chunk_duration_ms

    def _run(self) -> None:
        next_ts = self.current_chunk_timestamp()
        self._broadcast(next_ts)

        while not self._stop_event.is_set():
            next_ts += self._chunk_duration_ms
            while not self._stop_event.is_set():
                now_ms = int(time.time() * 1000)
                wait_ms = next_ts - now_ms
                if wait_ms <= 0:
                    break
                self._stop_event.wait(min(wait_ms / 1000.0, 0.5))

            if self._stop_event.is_set():
                break
            self._broadcast(next_ts)

    def _broadcast(self, timestamp_ms: int) -> None:
        self._logger.info("chunk started ts=%s", timestamp_ms)
        with self._lock:
            callbacks = list(self._callbacks)
        for callback in callbacks:
            callback(timestamp_ms)
