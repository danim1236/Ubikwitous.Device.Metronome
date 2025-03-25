#!/usr/bin/env python3

import gi
import time
import os
import threading

gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

Gst.init(None)

SEGMENT_DURATION = 30


class CameraRecorder:

    def __init__(self, name, rtsp_url):

        self.name = name
        self.rtsp_url = rtsp_url

        self.current_file = None
        self.segment_index = 0
        self.lock = threading.Lock()

        pipeline_str = f"""
        rtspsrc location={rtsp_url} latency=50 !
        rtph264depay !
        h264parse !
        nvv4l2decoder !
        nvvideoconvert !
        nvv4l2h264enc iframeinterval=600 insert-sps-pps=true bitrate=4000000 !
        h264parse !
        appsink name=sink emit-signals=true sync=false
        """

        self.pipeline = Gst.parse_launch(pipeline_str)

        self.appsink = self.pipeline.get_by_name("sink")
        self.appsink.connect("new-sample", self.on_sample)

    def start(self):

        self.open_new_chunk()
        self.pipeline.set_state(Gst.State.PLAYING)

    def on_sample(self, sink):

        sample = sink.emit("pull-sample")
        buffer = sample.get_buffer()

        success, mapinfo = buffer.map(Gst.MapFlags.READ)

        if success:

            with self.lock:
                if self.current_file:
                    self.current_file.write(mapinfo.data)

            buffer.unmap(mapinfo)

        return Gst.FlowReturn.OK

    def open_new_chunk(self):

        with self.lock:

            if self.current_file:
                self.current_file.close()

            filename = f"{self.name}_{self.segment_index:05d}.h264"
            print("open", filename)

            self.current_file = open(filename, "wb")

            self.segment_index += 1

    def rotate_chunk(self):

        self.open_new_chunk()

    def force_keyframe(self):

        event = Gst.Event.new_custom(
            Gst.EventType.CUSTOM_DOWNSTREAM,
            Gst.Structure.new_empty("GstForceKeyUnit")
        )

        self.pipeline.send_event(event)


class SegmentScheduler:

    def __init__(self, cameras):

        self.cameras = cameras

    def run(self):

        while True:

            now = time.time()

            next_boundary = (
                int(now / SEGMENT_DURATION) + 1
            ) * SEGMENT_DURATION

            sleep_time = next_boundary - now

            time.sleep(sleep_time)

            print("SEGMENT BOUNDARY")

            for cam in self.cameras:
                cam.force_keyframe()

            time.sleep(0.05)

            for cam in self.cameras:
                cam.rotate_chunk()


def main():

    cameras = [
        CameraRecorder(
            "cam1",
            "rtsp://127.0.0.1/stream1"
        ),
        CameraRecorder(
            "cam2",
            "rtsp://127.0.0.1/stream2"
        ),
    ]

    for cam in cameras:
        cam.start()

    scheduler = SegmentScheduler(cameras)

    t = threading.Thread(target=scheduler.run)
    t.daemon = True
    t.start()

    loop = GLib.MainLoop()
    loop.run()


if __name__ == "__main__":
    main()

