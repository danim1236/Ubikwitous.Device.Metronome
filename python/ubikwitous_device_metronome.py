#!/usr/bin/env python3

import gi
import time
import threading
gi.require_version("Gst", "1.0")

from gi.repository import Gst, GLib

Gst.init(None)

SEGMENT_DURATION = 30  # segundos


class CameraPipeline:

    def __init__(self, name, rtsp_url):
        self.name = name
        self.rtsp_url = rtsp_url

        pipeline_str = f"""
        rtspsrc location={rtsp_url} latency=50 !
        rtph264depay !
        h264parse !
        nvv4l2decoder !
        nvvideoconvert !
        nvv4l2h264enc iframeinterval=600 insert-sps-pps=true bitrate=4000000 !
        h264parse !
        splitmuxsink name=mux
            location={name}_%05d.mp4
            max-size-time={SEGMENT_DURATION * 1000000000}
        """

        self.pipeline = Gst.parse_launch(pipeline_str)
        self.encoder = self.pipeline.get_by_name("nvv4l2h264enc0")

    def start(self):
        self.pipeline.set_state(Gst.State.PLAYING)

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
            next_boundary = (int(now / SEGMENT_DURATION) + 1) * SEGMENT_DURATION

            sleep_time = next_boundary - now
            time.sleep(sleep_time)

            print("Segment boundary reached")

            for cam in self.cameras:
                cam.force_keyframe()


def main():

    cams = [
        CameraPipeline("cam1", "rtsp://127.0.0.1/stream1"),
        CameraPipeline("cam2", "rtsp://127.0.0.1/stream2"),
    ]

    for cam in cams:
        cam.start()

    scheduler = SegmentScheduler(cams)

    t = threading.Thread(target=scheduler.run)
    t.daemon = True
    t.start()

    loop = GLib.MainLoop()
    loop.run()


if __name__ == "__main__":
    main()


