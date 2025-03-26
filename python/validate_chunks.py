#!/usr/bin/env python3

import subprocess
import json
import os
import sys
import glob


EXPECTED_FPS = 25
EXPECTED_DURATION = 30


def run(cmd):

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )

    return result.stdout.strip()


def get_stream_info(path):

    cmd = [
        "ffprobe",
        "-v", "error",
        "-f", "h264",
        "-select_streams", "v:0",
        "-show_streams",
        "-print_format", "json",
        path
    ]

    out = run(cmd)

    try:
        data = json.loads(out)
        return data["streams"][0]
    except:
        return None


def count_frames(path):

    cmd = [
        "ffprobe",
        "-v", "error",
        "-f", "h264",
        "-count_frames",
        "-select_streams", "v:0",
        "-show_entries",
        "stream=nb_read_frames",
        "-print_format", "json",
        path
    ]

    out = run(cmd)

    try:
        data = json.loads(out)
        return int(data["streams"][0]["nb_read_frames"])
    except:
        return None


def starts_with_idr(path):

    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_frames",
        "-read_intervals", "%+#1",
        "-f", "h264",
        "-print_format", "json",
        path
    ]

    out = run(cmd)

    try:
        data = json.loads(out)
        frame = data["frames"][0]
        return frame.get("pict_type") == "I"
    except:
        return False


def validate_file(path):

    info = get_stream_info(path)

    if not info:
        print("BROKEN:", path)
        return

    width = info["width"]
    height = info["height"]

    fps = eval(info["avg_frame_rate"])

    frames = count_frames(path)

    if frames:
        duration = frames / fps
    else:
        duration = None

    idr = starts_with_idr(path)

    status = "OK"

    expected_frames = EXPECTED_FPS * EXPECTED_DURATION

    if frames and abs(frames - expected_frames) > 2:
        status = "FRAME_COUNT_MISMATCH"

    if not idr:
        status = "NO_IDR_START"

    print(
        f"{status:20} "
        f"{os.path.basename(path)} "
        f"{width}x{height} "
        f"fps={fps:.2f} "
        f"frames={frames} "
        f"duration={duration:.2f}"
    )


def main():

    if len(sys.argv) < 2:
        print("usage: validate_chunks.py <directory>")
        return

    path = sys.argv[1]

    files = sorted(glob.glob(os.path.join(path, "*.h264")))

    print("checking", len(files), "files\n")

    for f in files:
        validate_file(f)


if __name__ == "__main__":
    main()

