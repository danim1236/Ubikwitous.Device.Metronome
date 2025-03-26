"""Utilities for inspecting H264 Annex-B byte streams."""


def _iter_nal_unit_types(frame: bytes):
    """Yield NAL unit types from a frame in Annex-B format.

    The parser looks for 3-byte and 4-byte start codes and reads the byte
    immediately after the start code as NAL header.
    """
    index = 0
    length = len(frame)
    while index < length - 3:
        start_len = 0
        if frame[index : index + 4] == b"\x00\x00\x00\x01":
            start_len = 4
        elif frame[index : index + 3] == b"\x00\x00\x01":
            start_len = 3

        if start_len:
            header_pos = index + start_len
            if header_pos < length:
                yield frame[header_pos] & 0x1F
            index = header_pos + 1
            continue

        index += 1


def has_idr_nal(frame: bytes) -> bool:
    """Return True when frame includes at least one IDR NAL (type 5)."""
    for nal_type in _iter_nal_unit_types(frame):
        if nal_type == 5:
            return True
    return False
