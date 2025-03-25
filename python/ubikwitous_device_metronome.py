#!/usr/bin/env python3

import gi
import time
import socket
import threading
import queue
import struct

gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)

SEGMENT_DURATION = 30
UDP_PORT = 5005
WRITE_QUEUE = 200


def is_idr(data):
    for i in range(len(data) - 4):
        if data[i] == 0 and data[i+1] == 0 and data[i+2] == 0 and data[i+3] == 1:
            nal = data[i+4] & 0x1F
            if nal == 5:
                return True
    return False


class DiskWriter(threading.Thread):

    def __init__(self, name):
        super().__init__(daemon=True)
        self.name = name
        self.queue = queue.Queue(WRITE_QUEUE)
        self.file = None
        self.lock = threading.Lock()

    def open_file(self, ts):
        with self.lock:

            if self.file:
                self.file.close()

            filename = f"{self.name}_{ts:.3f}.h264"
            print("OPEN", filename)

            self.file = open(filename, "wb")

    def write(self, data):
        try:
            self.queue.put_nowait(data)
        except queue.Full:
            print(self.name, "disk queue overflow")

    def run(self):
        while True:
            data = self.queue.get()
            with self.lock:
                if self.file:
                    self.file.write(data)


class Camera:

    def __init__(self, name, url):

        self.name = name
        self.url = url

        self.pending_rotation = False

        self.writer = DiskWriter(name)
        self.writer.start()

        pipeline = f"""
        rtspsrc location={url} latency=80 !
        rtph264depay !
        h264parse !
        nvv4l2decoder !
        nvvideoconvert !
        nvv4l2h264enc iframeinterval=600 insert-sps-pps=true bitrate=4000000 !
        h264parse config-interval=-1 !
        appsink name=sink emit-signals=true sync=false
        """

        self.pipeline = Gst.parse_launch(pipeline)

        self.appsink = self.pipeline.get_by_name("sink")
        self.appsink.connect("new-sample", self.on_sample)

    def start(self):
        self.pipeline.set_state(Gst.State.PLAYING)

    def on_sample(self, sink):

        sample = sink.emit("pull-sample")
        buf = sample.get_buffer()

        ok, mapinfo = buf.map(Gst.MapFlags.READ)

        if not ok:
            return Gst.FlowReturn.OK

        data = mapinfo.data

        if self.pending_rotation and is_idr(data):

            pts = buf.pts / Gst.SECOND

            self.writer.open_file(pts)

            self.pending_rotation = False

        self.writer.write(data)

        buf.unmap(mapinfo)

        return Gst.FlowReturn.OK

    def force_keyframe(self):

        self.pending_rotation = True

        event = Gst.Event.new_custom(
            Gst.EventType.CUSTOM_DOWNSTREAM,
            Gst.Structure.new_empty("GstForceKeyUnit")
        )

        self.pipeline.send_event(event)


class SyncReceiver(threading.Thread):

    def __init__(self, cameras):
        super().__init__(daemon=True)
        self.cameras = cameras

    def run(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", UDP_PORT))

        while True:

            data, _ = sock.recvfrom(1024)

            if data == b"TICK":

                print("SYNC TICK")

                for c in self.cameras:
                    c.force_keyframe()


class SyncBroadcaster(threading.Thread):

    def run(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        while True:

            now = time.time()

            next_boundary = (
                int(now / SEGMENT_DURATION) + 1
            ) * SEGMENT_DURATION

            sleep = next_boundary - now

            time.sleep(sleep)

            sock.sendto(b"TICK", ("255.255.255.255", UDP_PORT))


def main():

    cameras = [

        Camera("cam1", "rtsp://127.0.0.1/stream1"),
        Camera("cam2", "rtsp://127.0.0.1/stream2"),
        Camera("cam3", "rtsp://127.0.0.1/stream3"),
        Camera("cam4", "rtsp://127.0.0.1/stream4"),

    ]

    for c in cameras:
        c.start()

    SyncReceiver(cameras).start()

    # apenas um Jetson deve executar isso
    # SyncBroadcaster().start()

    loop = GLib.MainLoop()
    loop.run()


if __name__ == "__main__":
    main()

