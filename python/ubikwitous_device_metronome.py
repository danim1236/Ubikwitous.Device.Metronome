#!/usr/bin/env python3

import gi
import yaml
import time
import socket
import argparse
import threading
import queue
from dataclasses import dataclass

gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)

WRITE_QUEUE_SIZE = 256


# -------------------------------------------------
# UTIL
# -------------------------------------------------

def is_idr(data):

    for i in range(len(data) - 4):
        if data[i:i+4] == b"\x00\x00\x00\x01":
            if (data[i+4] & 0x1F) == 5:
                return True
    return False


# -------------------------------------------------
# CONFIG
# -------------------------------------------------

@dataclass
class RecordingConfig:
    fps: int
    width: int
    height: int
    bitrate: int
    chunk_duration: int


@dataclass
class DeviceConfig:
    role: str
    udp_port: int


@dataclass
class CameraConfig:
    name: str
    rtsp: str


def load_config(path):

    with open(path) as f:
        raw = yaml.safe_load(f)

    rec = RecordingConfig(**raw["recording"])
    dev = DeviceConfig(**raw["device"])
    cams = [CameraConfig(**c) for c in raw["cameras"]]

    if rec.fps <= 0:
        raise RuntimeError("invalid fps")

    if rec.chunk_duration <= 0:
        raise RuntimeError("invalid chunk duration")

    if not cams:
        raise RuntimeError("no cameras configured")

    return rec, dev, cams


# -------------------------------------------------
# DISK WRITER
# -------------------------------------------------

class DiskWriter(threading.Thread):

    def __init__(self, name):

        super().__init__(daemon=True)

        self.name = name
        self.queue = queue.Queue(WRITE_QUEUE_SIZE)

        self.file = None
        self.lock = threading.Lock()
        self.first_write = False


    def open_chunk(self, ts):

        ts = int(ts * 1000)
        filename = f"chunk_{self.name}_{ts}.h264"

        with self.lock:

            if self.file:
                self.file.close()

            print(f"{self.name} OPEN FILE {filename}")

            self.file = open(filename, "wb")
            self.first_write = False

    def push(self, data):

        try:
            self.queue.put_nowait(data)
        except queue.Full:
            print(self.name, "disk queue overflow")

    def run(self):

        while True:

            data = self.queue.get()

            with self.lock:

                if self.file:

                    if not self.first_write:
                        print(self.name, "FIRST FRAME WRITTEN")
                        self.first_write = True

                    self.file.write(data)


# -------------------------------------------------
# CAMERA ENGINE
# -------------------------------------------------

class CameraEngine:

    def __init__(self, cfg: CameraConfig, rec: RecordingConfig):

        self.name = cfg.name
        self.rtsp = cfg.rtsp

        print("\n==============================")
        print("CAMERA:", self.name)
        print("RTSP  :", self.rtsp)
        print("==============================")

        self.rec = rec

        self.period = 1.0 / rec.fps
        self.gop = rec.fps * rec.chunk_duration

        self.latest_buffer = None
        self.lock = threading.Lock()

        self.pending_rotation = False

        self.decode_count = 0
        self.encode_count = 0
        self.last_stat = time.time()

        self.writer = DiskWriter(self.name)
        self.writer.start()

        self._build_decode()
        self._build_encode()

    # -------------------------------------------------

    def attach_bus_logger(self, pipeline):

        bus = pipeline.get_bus()
        bus.add_signal_watch()

        def on_message(bus, message):

            t = message.type

            if t == Gst.MessageType.ERROR:
                err, dbg = message.parse_error()
                print(f"[{self.name}] GST ERROR:", err, dbg)

            elif t == Gst.MessageType.WARNING:
                err, dbg = message.parse_warning()
                print(f"[{self.name}] GST WARNING:", err, dbg)

            elif t == Gst.MessageType.EOS:
                print(f"[{self.name}] GST EOS")

        bus.connect("message", on_message)

    # -------------------------------------------------

    def _build_decode(self):

        desc = f"""
        rtspsrc location={self.rtsp} latency=80 protocols=tcp !
        rtph264depay !
        h264parse !
        nvv4l2decoder !
        nvvideoconvert !
        video/x-raw(memory:NVMM),format=NV12 !
        appsink name=dec_sink emit-signals=true sync=false max-buffers=1 drop=true
        """

        self.decode_pipeline = Gst.parse_launch(desc)

        self.attach_bus_logger(self.decode_pipeline)

        sink = self.decode_pipeline.get_by_name("dec_sink")
        sink.connect("new-sample", self._on_decode)

    def _on_decode(self, sink):

        sample = sink.emit("pull-sample")
        buf = sample.get_buffer()

        if not hasattr(self, "decode_started"):
            self.decode_started = True
            print(self.name, "DECODE STARTED")

        self.decode_count += 1

        with self.lock:
            self.latest_buffer = buf.copy()

        return Gst.FlowReturn.OK

    # -------------------------------------------------

    def _build_encode(self):

        desc = f"""
        appsrc name=src is-live=true block=false format=time !
        video/x-raw(memory:NVMM),format=NV12,width={self.rec.width},height={self.rec.height},framerate={self.rec.fps}/1 !
        queue max-size-buffers=4 leaky=downstream !
        nvv4l2h264enc insert-sps-pps=true
                      iframeinterval={self.gop}
                      bitrate={self.rec.bitrate}
                      maxperf-enable=1
                      preset-level=1 !
        h264parse config-interval=-1 !
        appsink name=enc_sink emit-signals=true sync=false
        """

        self.encode_pipeline = Gst.parse_launch(desc)

        self.attach_bus_logger(self.encode_pipeline)

        self.appsrc = self.encode_pipeline.get_by_name("src")
        self.encsink = self.encode_pipeline.get_by_name("enc_sink")

        self.encsink.connect("new-sample", self._on_encoded)

        for e in self.encode_pipeline.iterate_elements():
            if e.get_factory().get_name() == "nvv4l2h264enc":
                self.encoder_pad = e.get_static_pad("sink")

    def _on_encoded(self, sink):

        sample = sink.emit("pull-sample")
        buf = sample.get_buffer()

        if not hasattr(self, "encode_started"):
            self.encode_started = True
            print(self.name, "ENCODE STARTED")

        self.encode_count += 1

        ok, mapinfo = buf.map(Gst.MapFlags.READ)

        if ok:

            data = mapinfo.data

            if self.pending_rotation and is_idr(data):

                ts = time.time()
                print(self.name, "ROTATE CHUNK AT", ts)

                self.writer.open_chunk(ts)

                self.pending_rotation = False

            self.writer.push(data)

            buf.unmap(mapinfo)

        return Gst.FlowReturn.OK

    # -------------------------------------------------

    def encode_tick(self, pts):

        with self.lock:
            buf = self.latest_buffer

        if not buf:
            return

        out = buf.copy()

        out.pts = int(pts * Gst.SECOND)
        out.duration = int(self.period * Gst.SECOND)

        self.appsrc.emit("push-buffer", out)

    # -------------------------------------------------

    def start(self):

        print(self.name, "STARTING PIPELINES")

        self.decode_pipeline.set_state(Gst.State.PLAYING)
        self.encode_pipeline.set_state(Gst.State.PLAYING)

    def force_keyframe(self):

        print(self.name, "FORCE KEYFRAME REQUESTED")

        self.pending_rotation = True

        event = Gst.Event.new_custom(
            Gst.EventType.CUSTOM_DOWNSTREAM,
            Gst.Structure.new_empty("GstForceKeyUnit")
        )

        self.encoder_pad.send_event(event)


# -------------------------------------------------
# GLOBAL PACER
# -------------------------------------------------

class GlobalPacer(threading.Thread):

    def __init__(self, cameras, fps):

        super().__init__(daemon=True)

        self.cameras = cameras
        self.period = 1.0 / fps

    def run(self):

        pts = 0
        tick = 0

        while True:

            start = time.time()

            for cam in self.cameras:
                cam.encode_tick(pts)

            pts += self.period
            tick += 1

            if tick % int(5 / self.period) == 0:
                print("PACER alive")

            elapsed = time.time() - start
            sleep = self.period - elapsed

            if sleep > 0:
                time.sleep(sleep)


# -------------------------------------------------
# STATS
# -------------------------------------------------

class StatsThread(threading.Thread):

    def __init__(self, cameras):

        super().__init__(daemon=True)
        self.cameras = cameras

    def run(self):

        while True:

            time.sleep(5)

            print("\n===== CAMERA STATS =====")

            for cam in self.cameras:

                now = time.time()
                dt = now - cam.last_stat

                decode_fps = cam.decode_count / dt
                encode_fps = cam.encode_count / dt

                print(
                    f"{cam.name} "
                    f"decode={decode_fps:.1f}fps "
                    f"encode={encode_fps:.1f}fps"
                )

                cam.decode_count = 0
                cam.encode_count = 0
                cam.last_stat = now


# -------------------------------------------------
# SYNC
# -------------------------------------------------

class SyncReceiver(threading.Thread):

    def __init__(self, cameras, port):

        super().__init__(daemon=True)

        self.cameras = cameras
        self.port = port

    def run(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", self.port))

        while True:

            data, _ = sock.recvfrom(1024)

            if data == b"TICK":

                print("SYNC TICK RECEIVED")

                for cam in self.cameras:
                    cam.force_keyframe()


class SyncBroadcaster(threading.Thread):

    def __init__(self, port, chunk_duration):

        super().__init__(daemon=True)

        self.port = port
        self.chunk_duration = chunk_duration

    def run(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        while True:

            now = time.time()

            boundary = (
                int(now / self.chunk_duration) + 1
            ) * self.chunk_duration

            time.sleep(boundary - now)

            print("SYNC TICK BROADCAST")

            sock.sendto(b"TICK", ("255.255.255.255", self.port))


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)

    args = parser.parse_args()

    rec, dev, cams = load_config(args.config)

    cameras = [CameraEngine(c, rec) for c in cams]

    for cam in cameras:
        cam.start()

    pacer = GlobalPacer(cameras, rec.fps)
    pacer.start()

    StatsThread(cameras).start()

    SyncReceiver(cameras, dev.udp_port).start()

    if dev.role == "master":
        SyncBroadcaster(dev.udp_port, rec.chunk_duration).start()

    loop = GLib.MainLoop()
    loop.run()


if __name__ == "__main__":
    main()

