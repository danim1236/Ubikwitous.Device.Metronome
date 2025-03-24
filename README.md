# Ubikwitous.Device.Metronome

Ubikwitous.Device.Metronome is the deterministic video ingestion layer of the Ubikwitous platform.

Its role is to transform unstable RTSP camera streams into **time-normalized video streams** with a deterministic frame timeline.

This component runs on edge devices (e.g. Jetson) and provides synchronized video chunks that can be processed later by analytics pipelines such as DeepStream or MV3DT.

---

# Core Concept

IP cameras are not deterministic.

Typical issues include:

- variable frame rate
- jitter
- dropped frames
- network burst
- clock drift

Ubikwitous.Device.Metronome solves this by introducing a **device-level clock**.

RTSP streams
    ↓
decoder
    ↓
latest_frame buffer
    ↓
Metronome frame pacer (deterministic FPS)
    ↓
encoder
    ↓
synchronized video chunks


