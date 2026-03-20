#!/usr/bin/env python3

import glob
import json
import os
import subprocess
import sys

EXPECTED_FPS = 25
EXPECTED_DURATION = 30


def run(cmd):
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def parse_fraction(value):
    if not value:
        return None
    parts = value.split("/")
    if len(parts) != 2:
        return None
    try:
        numerator = float(parts[0])
        denominator = float(parts[1])
    except ValueError:
        return None
    if denominator == 0:
        return None
    return numerator / denominator


def get_stream_info(path):
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-f",
        "h264",
        "-select_streams",
        "v:0",
        "-show_streams",
        "-print_format",
        "json",
        path,
    ]
    code, out, _ = run(cmd)
    if code != 0:
        return None
    try:
        data = json.loads(out)
    except ValueError:
        return None
    streams = data.get("streams")
    if not streams:
        return None
    return streams[0]


def count_frames(path):
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-f",
        "h264",
        "-count_frames",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=nb_read_frames",
        "-print_format",
        "json",
        path,
    ]
    code, out, _ = run(cmd)
    if code != 0:
        return None

    try:
        data = json.loads(out)
        return int(data["streams"][0]["nb_read_frames"])
    except (ValueError, KeyError, IndexError, TypeError):
        return None


def validate_file(path):
    info = get_stream_info(path)
    if not info:
        print("BROKEN: {0}".format(path))
        return

    width = info.get("width")
    height = info.get("height")
    fps = parse_fraction(info.get("avg_frame_rate"))
    frames = count_frames(path)

    duration = None
    if fps and frames is not None:
        duration = float(frames) / fps

    status = "OK"
    expected_frames = EXPECTED_FPS * EXPECTED_DURATION
    if frames is None:
        status = "UNKNOWN_FRAME_COUNT"
    elif abs(frames - expected_frames) > 2:
        status = "FRAME_COUNT_MISMATCH"

    duration_text = "n/a"
    if duration is not None:
        duration_text = "{0:.2f}".format(duration)

    fps_text = "n/a"
    if fps is not None:
        fps_text = "{0:.2f}".format(fps)

    print(
        "{0:20} {1} {2}x{3} fps={4} frames={5} duration={6}".format(
            status,
            os.path.basename(path),
            width,
            height,
            fps_text,
            frames,
            duration_text,
        )
    )


def main():
    if len(sys.argv) < 2:
        print("usage: validate_chunks.py <directory>")
        return

    path = sys.argv[1]
    files = sorted(glob.glob(os.path.join(path, "*.h264")))

    print("checking {0} files\n".format(len(files)))
    for chunk_file in files:
        validate_file(chunk_file)


if __name__ == "__main__":
    main()
