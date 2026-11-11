# -*- coding: utf-8 -*-
"""Independent wire-format fixture builder for the kafka-python CRC audit.

Everything in this module is built from the published Kafka wire format
specification using only ``struct``/``gzip`` and two locally written,
table-driven CRC implementations. None of kafka-python's record builders
(MemoryRecordsBuilder / LegacyRecordBatchBuilder / DefaultRecordBatchBuilder)
are used, so expected values (including expected CRCs) come from an
independent oracle, not from the code under test.

kafka-python is only ever used to *parse* the bytes produced here.
"""

import gzip
import struct
import zlib


# ---------------------------------------------------------------------------
# Independent CRC implementations (written from the polynomial definitions)
# ---------------------------------------------------------------------------

def _make_table(poly_reflected):
    table = []
    for n in range(256):
        c = n
        for _ in range(8):
            c = (c >> 1) ^ (poly_reflected if c & 1 else 0)
        table.append(c)
    return table


# CRC-32 (ISO-HDLC / Ethernet), reflected polynomial 0xEDB88320,
# init 0xFFFFFFFF, xorout 0xFFFFFFFF. Used by legacy message format v0/v1.
_CRC32_ISO_TABLE = _make_table(0xEDB88320)


def crc32_iso(data):
    crc = 0xFFFFFFFF
    for byte in bytes(data):
        crc = _CRC32_ISO_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


# CRC-32C (Castagnoli), reflected polynomial 0x82F63B78,
# init 0xFFFFFFFF, xorout 0xFFFFFFFF. Used by record batch format v2.
_CRC32C_TABLE = _make_table(0x82F63B78)


def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in bytes(data):
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def self_check():
    """Validate the independent oracle against published check values and
    cross-check against third-party implementations (zlib, kafka-python's
    own CRC32C) on pseudo-random inputs. Cross-checking only validates the
    oracle; no fixture answer is derived from kafka-python."""
    assert crc32_iso(b"123456789") == 0xCBF43926, hex(crc32_iso(b"123456789"))
    assert crc32c(b"123456789") == 0xE3069283, hex(crc32c(b"123456789"))
    from kafka.record.util import calc_crc32c, calc_crc32
    for seed in range(50):
        blob = bytes(((seed * 31 + i * 17) & 0xFF) for i in range(seed + 1))
        assert crc32_iso(blob) == (zlib.crc32(blob) & 0xFFFFFFFF)
        assert crc32_iso(blob) == calc_crc32(memoryview(blob))
        assert crc32c(blob) == calc_crc32c(memoryview(blob))
    return True


# ---------------------------------------------------------------------------
# Varint (zig-zag) encoding for v2 records -- independent implementation
# ---------------------------------------------------------------------------

def varint(value):
    zz = (value << 1) ^ (value >> 63)
    out = bytearray()
    while True:
        bits = zz & 0x7F
        zz >>= 7
        if zz:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


# ---------------------------------------------------------------------------
# Legacy message format v0 / v1 (MessageSet entries)
#
#  Offset => Int64            <- NOT covered by CRC
#  MessageSize => Int32       <- NOT covered by CRC
#  Crc => Int32               (CRC-32/ISO-HDLC over Magic..end of Value)
#  Magic => Int8              -+
#  Attributes => Int8          |
#  Timestamp => Int64 (v1)     | CRC coverage
#  KeyLength => Int32          |
#  Key => Bytes                |
#  ValueLength => Int32        |
#  Value => Bytes             -+
# ---------------------------------------------------------------------------

LOG_OVERHEAD = 12  # offset(8) + size(4)


def legacy_message(magic, offset, key, value, attrs=0, timestamp=1700000000000,
                   crc_override=None):
    body = struct.pack(">bb", magic, attrs)
    if magic >= 1:
        body += struct.pack(">q", timestamp)
    body += struct.pack(">i", -1 if key is None else len(key))
    if key is not None:
        body += key
    body += struct.pack(">i", -1 if value is None else len(value))
    if value is not None:
        body += value
    crc = crc32_iso(body) if crc_override is None else crc_override
    msg = struct.pack(">I", crc) + body  # kafka-python reads legacy CRC as uint32
    return struct.pack(">qi", offset, len(msg)) + msg


def legacy_gzip_batch(magic, first_offset, items, timestamp=1700000000000,
                      inner_corrupt_index=None):
    """Wrapper message whose value is a gzip-compressed inner MessageSet.

    inner_corrupt_index: if set, that inner message gets a deliberately
    wrong CRC (models corruption that happened *before* compression, so
    the outer CRC is still computed correctly over the compressed bytes).
    """
    inner = b""
    for i, (key, value) in enumerate(items):
        off = i if magic >= 1 else first_offset + i
        crc_override = None
        if inner_corrupt_index == i:
            good = legacy_message(magic, off, key, value, timestamp=timestamp)
            crc_override = struct.unpack(">I", good[12:16])[0] ^ 0xFF  # wrong CRC
        inner += legacy_message(magic, off, key, value, timestamp=timestamp,
                                crc_override=crc_override)
    compressed = gzip.compress(inner, mtime=0)
    wrapper_offset = first_offset + len(items) - 1
    return legacy_message(magic, wrapper_offset, None, compressed,
                          attrs=0x01, timestamp=timestamp)


# ---------------------------------------------------------------------------
# Default record batch format v2
#
#  BaseOffset => Int64            <- NOT covered by CRC
#  Length => Int32                <- NOT covered by CRC
#  PartitionLeaderEpoch => Int32  <- NOT covered by CRC
#  Magic => Int8                  <- NOT covered by CRC
#  Crc => Uint32                  (CRC-32C over Attributes..end of records)
#  Attributes => Int16            -+
#  LastOffsetDelta => Int32        |
#  FirstTimestamp => Int64         |
#  MaxTimestamp => Int64           |
#  ProducerId => Int64             | CRC-32C coverage
#  ProducerEpoch => Int16          |
#  BaseSequence => Int32           |
#  RecordsCount => Int32           |
#  Records => [Record ...]        -+
# ---------------------------------------------------------------------------

V2_BASE_OFFSET = 0
V2_LENGTH = 8
V2_LEADER_EPOCH = 12
V2_MAGIC = 16
V2_CRC = 17
V2_ATTRS = 21
V2_LAST_OFFSET_DELTA = 23
V2_FIRST_TS = 27
V2_RECORDS_COUNT = 57
V2_RECORDS_START = 61


def v2_record(offset_delta, ts_delta, key, value, headers=()):
    body = b"\x00"  # record attributes (unused in v2)
    body += varint(ts_delta)
    body += varint(offset_delta)
    body += varint(-1 if key is None else len(key))
    if key is not None:
        body += key
    body += varint(-1 if value is None else len(value))
    if value is not None:
        body += value
    body += varint(len(headers))
    for h_key, h_val in headers:
        h_key_b = h_key.encode("utf-8")
        body += varint(len(h_key_b)) + h_key_b
        body += varint(-1 if h_val is None else len(h_val))
        if h_val is not None:
            body += h_val
    return varint(len(body)) + body


def v2_batch(records, base_offset=0, leader_epoch=0, attrs=0,
             first_ts=1700000000000, max_ts=1700000000000,
             last_offset_delta=None, compression=None, crc_override=None):
    payload = b"".join(records)
    if compression == "gzip":
        payload = gzip.compress(payload, mtime=0)
        attrs = (attrs & ~0x07) | 0x01
    if last_offset_delta is None:
        last_offset_delta = len(records) - 1
    body = struct.pack(">h", attrs)
    body += struct.pack(">i", last_offset_delta)
    body += struct.pack(">q", first_ts)
    body += struct.pack(">q", max_ts)
    body += struct.pack(">q", -1)   # ProducerId
    body += struct.pack(">h", -1)   # ProducerEpoch
    body += struct.pack(">i", -1)   # BaseSequence
    body += struct.pack(">i", len(records))
    body += payload
    crc = crc32c(body) if crc_override is None else crc_override
    batch_len = 4 + 1 + 4 + len(body)  # leaderEpoch + magic + crc + body
    return (struct.pack(">qiib", base_offset, batch_len, leader_epoch, 2)
            + struct.pack(">I", crc) + body)


def flip(buf, pos):
    """Return a copy of buf with the byte at pos XORed with 0xFF."""
    data = bytearray(buf)
    data[pos] ^= 0xFF
    return bytes(data)


def poke_int(buf, pos, fmt, value):
    """Return a copy of buf with struct fmt written at pos."""
    data = bytearray(buf)
    struct.pack_into(fmt, data, pos, value)
    return bytes(data)
