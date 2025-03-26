# Ubikwitous.Device.Metronome

Synchronized multi-camera RTSP chunk recorder for H264 streams.

## Design goals

- No decoding, encoding, transcoding, or bitstream modification.
- Low CPU usage suitable for Raspberry Pi / Jetson Nano.
- Global chunk synchronization across all cameras.
- Robust reconnection for long-running edge processes.

## Pipeline

Per camera, the recorder uses exactly:

`rtspsrc protocols=tcp -> rtph264depay -> h264parse -> appsink`

Each appsink buffer is treated as one frame boundary (provided by `h264parse`) and written directly to disk.

## Configuration

Run with:

```bash
python recorder.py config.yaml
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
- Active file: `recordings/chunk_{timestamp}_{camera}.tmp`
- Finalized file: `recordings/chunk_{timestamp}_{camera}_{skip}.h264`

`skip` is the number of pre-IDR frames before the first IDR (NAL type 5) appears in the chunk.

## Modules

- `recorder.py` - entrypoint, lifecycle, signal handling.
- `config_loader.py` - YAML loading and validation.
- `scheduler.py` - global chunk rotation broadcaster.
- `camera_stream.py` - RTSP ingest, frame handling, reconnect.
- `h264_utils.py` - Annex-B NAL parsing and IDR detection.
- `chunk_writer.py` - sequential file writing and finalization.

## Logging

The recorder logs:

- camera started
- rtsp connected
- rtsp disconnected
- chunk started
- chunk closed
- reconnect attempts

## Shutdown

On `SIGINT`:

- scheduler stops
- pipelines transition to `NULL`
- open `.tmp` files are finalized and renamed
- process exits cleanly
