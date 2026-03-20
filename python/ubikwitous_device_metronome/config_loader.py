"""Configuration loading and validation for the synchronized RTSP recorder."""

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


@dataclass(frozen=True)
class RecordingConfig:
    """Recording metadata values used for scheduler and naming."""

    fps: int
    chunk_duration: int
    bitrate: int
    width: int
    height: int
    chunk_duration_ms: int
    frame_interval_ms: float


@dataclass(frozen=True)
class CameraConfig:
    """Camera RTSP connection configuration."""

    name: str
    rtsp: str


@dataclass(frozen=True)
class AppConfig:
    """Top-level application configuration."""

    recording: RecordingConfig
    cameras: List[CameraConfig]


def _require_positive_int(data: dict, key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"recording.{key} must be a positive integer")
    return value


def load_config(path: str) -> AppConfig:
    """Load and validate application configuration from YAML."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping")

    recording_raw = raw.get("recording")
    if not isinstance(recording_raw, dict):
        raise ValueError("recording section is required")

    fps = _require_positive_int(recording_raw, "fps")
    chunk_duration = _require_positive_int(recording_raw, "chunk_duration")
    bitrate = _require_positive_int(recording_raw, "bitrate")
    width = _require_positive_int(recording_raw, "width")
    height = _require_positive_int(recording_raw, "height")

    recording = RecordingConfig(
        fps=fps,
        chunk_duration=chunk_duration,
        bitrate=bitrate,
        width=width,
        height=height,
        chunk_duration_ms=chunk_duration * 1000,
        frame_interval_ms=1000.0 / fps,
    )

    cameras_raw = raw.get("cameras")
    if not isinstance(cameras_raw, list) or not cameras_raw:
        raise ValueError("cameras must be a non-empty list")

    camera_names = set()
    cameras: List[CameraConfig] = []
    for index, item in enumerate(cameras_raw):
        if not isinstance(item, dict):
            raise ValueError(f"cameras[{index}] must be a mapping")

        name = item.get("name")
        rtsp = item.get("rtsp")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"cameras[{index}].name must be a non-empty string")
        if "_" in name:
            raise ValueError(f"cameras[{index}].name must not contain underscore: {name}")
        if name in camera_names:
            raise ValueError(f"Duplicate camera name: {name}")

        if not isinstance(rtsp, str) or not rtsp.strip():
            raise ValueError(f"cameras[{index}].rtsp must be a non-empty string")

        camera_names.add(name)
        cameras.append(CameraConfig(name=name, rtsp=rtsp))

    return AppConfig(recording=recording, cameras=cameras)
