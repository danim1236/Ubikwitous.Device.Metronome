"""RTSP camera ingestion and per-camera chunk file handling."""

import logging
import threading
import time
from pathlib import Path
from typing import Optional

from gi.repository import Gst

from chunk_writer import ChunkWriter


class CameraStream:
    """Represents one RTSP H264 stream and chunk writer state."""

    def __init__(self, name: str, rtsp_url: str, output_dir: Path, logger: logging.Logger) -> None:
        self.name = name
        self.rtsp_url = rtsp_url
        self._logger = logger
        self._writer = ChunkWriter(output_dir=output_dir, camera_name=name)

        self.current_chunk_timestamp = None  # type: Optional[int]

        self._lock = threading.Lock()
        self._running = False
        self._connected = False
        self._reconnect_event = threading.Event()
        self._reconnect_thread = threading.Thread(target=self._reconnect_loop, name="reconnect-{0}".format(name), daemon=True)

        self._pipeline = self._build_pipeline()
        self._bus = self._pipeline.get_bus()
        self._bus.add_signal_watch()
        self._bus.connect("message", self._on_bus_message)

        self._appsink = self._pipeline.get_by_name("sink")
        self._appsink.connect("new-sample", self._on_new_sample)

    def start(self) -> None:
        self._running = True
        self._reconnect_thread.start()
        self._logger.info("camera started name=%s", self.name)
        self._set_pipeline_playing()

    def stop(self) -> None:
        self._running = False
        self._reconnect_event.set()
        if self._reconnect_thread.is_alive():
            self._reconnect_thread.join(timeout=3)

        self._pipeline.set_state(Gst.State.NULL)
        with self._lock:
            final_path = self._writer.close_and_finalize()
            if final_path is not None:
                self._logger.info("chunk closed camera=%s file=%s", self.name, final_path.name)

    def rotate_event(self, timestamp_ms: int) -> None:
        """Rotate chunk file in global scheduler order."""
        with self._lock:
            if self.current_chunk_timestamp is not None:
                closed = self._writer.close_and_finalize()
                if closed is not None:
                    self._logger.info("chunk closed camera=%s file=%s", self.name, closed.name)

            self._writer.open_chunk(timestamp_ms)
            self.current_chunk_timestamp = timestamp_ms
            self._logger.info("chunk started camera=%s ts=%s", self.name, timestamp_ms)

    def _build_pipeline(self) -> Gst.Pipeline:
        launch = (
            'rtspsrc location="{0}" protocols=tcp name=src '.format(self.rtsp_url)
            + "! rtph264depay "
            + "! h264parse config-interval=1 "
            + "! video/x-h264,stream-format=byte-stream "
            + "! appsink name=sink emit-signals=true sync=false max-buffers=1 drop=true enable-last-sample=false "
        )
        pipeline = Gst.parse_launch(launch)
        if not isinstance(pipeline, Gst.Pipeline):
            raise RuntimeError("Failed to create pipeline for {0}".format(self.name))
        return pipeline

    def _set_pipeline_playing(self) -> None:
        state_change = self._pipeline.set_state(Gst.State.PLAYING)
        if state_change == Gst.StateChangeReturn.FAILURE:
            self._logger.warning("rtsp disconnected camera=%s", self.name)
            self._schedule_reconnect()

    def _on_new_sample(self, sink: Gst.Element) -> Gst.FlowReturn:
        sample = sink.emit("pull-sample")
        if sample is None:
            return Gst.FlowReturn.OK

        buffer = sample.get_buffer()
        ok, map_info = buffer.map(Gst.MapFlags.READ)
        if not ok:
            return Gst.FlowReturn.OK

        try:
            frame = bytes(map_info.data)
        finally:
            buffer.unmap(map_info)

        with self._lock:
            if self.current_chunk_timestamp is None:
                return Gst.FlowReturn.OK
            self._writer.write_frame(frame)

        return Gst.FlowReturn.OK

    def _on_bus_message(self, bus: Gst.Bus, message: Gst.Message) -> None:
        message_type = message.type

        if message_type == Gst.MessageType.ERROR:
            error, debug = message.parse_error()
            self._logger.warning(
                "rtsp disconnected camera=%s error=%s debug=%s",
                self.name,
                error,
                debug,
            )
            self._connected = False
            self._schedule_reconnect()
        elif message_type == Gst.MessageType.EOS:
            self._logger.warning("rtsp disconnected camera=%s reason=eos", self.name)
            self._connected = False
            self._schedule_reconnect()
        elif message_type == Gst.MessageType.STATE_CHANGED and message.src == self._pipeline:
            old_state, new_state, _ = message.parse_state_changed()
            if new_state == Gst.State.PLAYING and old_state != Gst.State.PLAYING:
                if not self._connected:
                    self._connected = True
                    self._logger.info("rtsp connected camera=%s", self.name)

    def _schedule_reconnect(self) -> None:
        if self._running:
            self._reconnect_event.set()

    def _reconnect_loop(self) -> None:
        while self._running:
            triggered = self._reconnect_event.wait(timeout=1.0)
            if not self._running:
                return
            if not triggered:
                continue

            self._reconnect_event.clear()
            attempt = 1
            while self._running and not self._connected:
                self._logger.info("reconnect attempt camera=%s attempt=%s", self.name, attempt)
                self._pipeline.set_state(Gst.State.NULL)
                time.sleep(0.5)
                self._set_pipeline_playing()
                attempt += 1
                time.sleep(2.0)
