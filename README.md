# Ubikwitous.Device.Metronome

Synchronized multi-camera RTSP chunk recorder for raw H264 streams.

## Design goals

- No decoding, encoding, transcoding, or bitstream modification.
- No keyframe, IDR, GOP, or skip analysis on the edge.
- Low CPU usage suitable for Raspberry Pi / Jetson Nano.
- Global chunk synchronization across all cameras.
- Robust reconnection for long-running edge processes.

## Edge recorder responsibilities

The edge recorder is intentionally limited to:

- RTSP ingest
- synchronized time-based chunk rotation
- raw H264 byte-stream writing

Any GOP/keyframe analysis is intentionally deferred to server-side processing.

## Pipeline

Per camera, the recorder uses exactly:

`rtspsrc protocols=tcp -> rtph264depay -> h264parse -> video/x-h264,stream-format=byte-stream -> appsink`

Each appsink buffer is written directly to the active chunk file in H264 Annex-B byte-stream format.

## Configuration

Run with:

```bash
python ubikwitous_device_metronome.py config.yaml
```

Example `config.yaml`:

```yaml
recording:
  fps: 20
  chunk_duration: 30
  bitrate: 4000000
  width: 1280
  height: 720

cameras:
  - name: cam1
    rtsp: rtsp://user:pass@192.168.1.10/stream
  - name: cam2
    rtsp: rtsp://user:pass@192.168.1.11/stream
```

Validation rules:

- camera names must be unique
- camera names must not contain `_`

Derived values:

- `chunk_duration_ms = chunk_duration * 1000`
- `frame_interval_ms = 1000 / fps`

## Chunking and filenames

- Scheduler timestamp: `floor(epoch_ms / chunk_duration_ms) * chunk_duration_ms`
- Active file: `recordings/chunk_{timestamp_ms}_{camera}.tmp`
- Finalized file: `recordings/chunk_{timestamp_ms}_{camera}.h264`

All cameras rotate using the same global scheduler timestamps.

Example output:

- `chunk_1773364980000_cam1.h264`
- `chunk_1773364980000_cam2.h264`
- `chunk_1773364980000_cam3.h264`
- `chunk_1773365010000_cam1.h264`
- `chunk_1773365010000_cam2.h264`
- `chunk_1773365010000_cam3.h264`

## Modules

- `ubikwitous_device_metronome.py` - entrypoint, lifecycle, signal handling.
- `config_loader.py` - YAML loading and validation.
- `scheduler.py` - global chunk rotation broadcaster.
- `camera_stream.py` - RTSP ingest, sample handling, reconnect.
- `chunk_writer.py` - sequential file writing and finalization.

## Logging

The recorder logs:

- camera started
- rtsp connected
- rtsp disconnected
- chunk started
- chunk closed
- reconnect attempts

Chunk close logs include camera and filename only; no keyframe-related fields.

## Shutdown

On `SIGINT`:

- scheduler stops
- pipelines transition to `NULL`
- open `.tmp` files are finalized and renamed to `.h264`
- process exits cleanly
