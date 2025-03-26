"""Sequential chunk file writing with tmp-to-final rename semantics."""

from pathlib import Path
from typing import Optional


class ChunkWriter:
    """Manages chunk file lifecycle for one camera."""

    def __init__(self, output_dir: Path, camera_name: str) -> None:
        self._output_dir = output_dir
        self._camera_name = camera_name
        self._file: Optional[object] = None
        self._timestamp: Optional[int] = None

    @property
    def chunk_timestamp(self) -> Optional[int]:
        return self._timestamp

    def open_chunk(self, timestamp_ms: int) -> None:
        """Open a new temporary file for the given chunk timestamp."""
        self.close_and_finalize(skip_count=0)

        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._timestamp = timestamp_ms
        tmp_path = self._tmp_path(timestamp_ms)
        self._file = tmp_path.open("wb", buffering=0)

    def write_frame(self, frame: bytes) -> None:
        """Write one frame payload to current chunk file."""
        if self._file is None:
            raise RuntimeError("Chunk file not opened")
        self._file.write(frame)

    def close_and_finalize(self, skip_count: int) -> Optional[Path]:
        """Close and rename current tmp file to final h264 file."""
        if self._file is None or self._timestamp is None:
            return None

        self._file.close()
        self._file = None

        tmp_path = self._tmp_path(self._timestamp)
        final_path = self._final_path(self._timestamp, skip_count)
        if tmp_path.exists():
            tmp_path.rename(final_path)

        self._timestamp = None
        return final_path

    def _tmp_path(self, timestamp_ms: int) -> Path:
        filename = f"chunk_{timestamp_ms}_{self._camera_name}.tmp"
        return self._output_dir / filename

    def _final_path(self, timestamp_ms: int, skip_count: int) -> Path:
        filename = f"chunk_{timestamp_ms}_{self._camera_name}_{skip_count}.h264"
        return self._output_dir / filename
