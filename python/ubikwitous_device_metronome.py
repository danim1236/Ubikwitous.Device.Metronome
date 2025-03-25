#!/usr/bin/env python3

import gi
import time
import threading
import queue
import os

gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)

SEGMENT_DURATION = 30
WRITE_QUEUE = 200


class DiskWriter(threading.Thread):

    def __init__(self, name):

        super().__init__(daemon=True)

        self.name = name
        self.queue = queue.Queue(WRITE_QUEUE)
        self.file = None
        self.index = 0
        self.lock = threading.Lock()

    def open_file(self):

        with self.lock:

            if self.file:
                self.file.close()

            filename = f"{self.name}_{self.index:05d}.h264"
            print("OPEN", filename)

            self.file = open(filename, "wb")
            self.index += 1

    def write(self, data):

        try:
            self.queue.put_nowait(data)
        except queue.Full:
            print(self.name, "disk queue overflow")

    def rotate(self):

        self.open_file()

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

        self.writer = DiskWriter(name)
        self.writer.start()

        pipeline = f"""
        rtspsrc location={url} latency=80 !
        rtph264depay !
        h264parse !
        nvv4l2decoder !
        nvvideoconvert !
        nvv4l2h264enc iframeinterval=600 insert-sps-pps=true bitrate=4000000 !
        h264parse !
        appsink name=sink emit-signals=true sync=false
        """

        self.pipeline = Gst.parse_launch(pipeline)

        self.appsink = self.pipeline.get_by_name("sink")
        self.appsink.connect("new-sample", self.on_sample)

    def start(self):

        self.writer.open_file()
        self.pipeline.set_state(Gst.State.PLAYING)

    def on_sample(self, sink):

        sample = sink.emit("pull-sample")
        buf = sample.get_buffer()

        ok, mapinfo = buf.map(Gst.MapFlags.READ)

        if ok:
            self.writer.write(mapinfo.data)
            buf.unmap(mapinfo)

        return Gst.FlowReturn.OK

    def rotate(self):

        self.writer.rotate()

    def force_keyframe(self):

        event = Gst.Event.new_custom(
            Gst.EventType.CUSTOM_DOWNSTREAM,
            Gst.Structure.new_empty("GstForceKeyUnit")
        )

        self.pipeline.send_event(event)


class Scheduler(threading.Thread):

    def __init__(self, cams):

        super().__init__(daemon=True)
        self.cams = cams

    def run(self):

        while True:

            now = time.time()

            boundary = (int(now / SEGMENT_DURATION) + 1) * SEGMENT_DURATION

            sleep = boundary - now

            time.sleep(sleep)

            print("SEGMENT", boundary)

            for c in self.cams:
                c.force_keyframe()

            time.sleep(0.05)

            for c in self.cams:
                c.rotate()


def main():

    cams = [

        Camera("cam1", "rtsp://127.0.0.1/stream1"),
        Camera("cam2", "rtsp://127.0.0.1/stream2"),
        Camera("cam3", "rtsp://127.0.0.1/stream3"),
        Camera("cam4", "rtsp://127.0.0.1/stream3"),

    ]

    for c in cams:
        c.start()

    scheduler = Scheduler(cams)
    scheduler.start()

    loop = GLib.MainLoop()
    loop.run()


if __name__ == "__main__":
    main()

