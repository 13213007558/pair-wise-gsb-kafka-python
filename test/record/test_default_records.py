# -*- coding: utf-8 -*-

import struct
from unittest.mock import patch

import pytest

import kafka.codec
from kafka.codec import gzip_encode
from kafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder
)
from kafka.record.util import calc_crc32c, encode_varint
from kafka.errors import CorruptRecordError, UnsupportedCodecError

from test.testutil import maybe_skip_unsupported_compression


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
    DefaultRecordBatch.CODEC_SNAPPY,
    DefaultRecordBatch.CODEC_LZ4
])
def test_read_write_serde_v2(compression_type):
    maybe_skip_unsupported_compression(compression_type)
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=1,
        producer_id=123456, producer_epoch=123, base_sequence=9999,
        batch_size=999999)
    headers = [("header1", b"aaa"), ("header2", b"bbb")]
    for offset in range(10):
        builder.append(
            offset, timestamp=9999999, key=b"test", value=b"Super",
            headers=headers)
    buffer = builder.build()
    reader = DefaultRecordBatch(bytes(buffer))
    msgs = list(reader)

    assert reader.is_transactional is True
    assert reader.compression_type == compression_type
    assert reader.magic == 2
    assert reader.timestamp_type == 0
    assert reader.base_offset == 0
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == 9999999
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.headers == headers


def test_written_bytes_equals_size_in_bytes_v2():
    key = b"test"
    value = b"Super"
    headers = [("header1", b"aaa"), ("header2", b"bbb"), ("xx", None)]
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=999999)

    size_in_bytes = DefaultRecordBatchBuilder.size_in_bytes(
        offset_delta=0, timestamp_delta=0, key=key, value=value, headers=headers)

    pos = builder.size()
    meta = builder.append(
        0, timestamp=9999999, key=key, value=value, headers=headers)

    assert builder.size() - pos == size_in_bytes
    assert meta.size == size_in_bytes


def test_estimate_size_in_bytes_bigger_than_batch_v2():
    key = b"Super Key"
    value = b"1" * 100
    headers = [("header1", b"aaa"), ("header2", b"bbb")]
    estimate_size = DefaultRecordBatchBuilder.estimate_size_in_bytes(
        key, value, headers)

    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=999999)
    builder.append(
        0, timestamp=9999999, key=key, value=value, headers=headers)
    buf = builder.build()
    assert len(buf) <= estimate_size, \
        "Estimate should always be upper bound"


def test_default_batch_builder_validates_arguments():
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=999999)

    # Key should not be str
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp=9999999, key="some string", value=None, headers=[])

    # Value should not be str
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp=9999999, key=None, value="some string", headers=[])

    # Timestamp should be of proper type
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp="1243812793", key=None, value=b"some string",
            headers=[])

    # Offset of invalid type
    with pytest.raises(TypeError):
        builder.append(
            "0", timestamp=9999999, key=None, value=b"some string", headers=[])

    # Ok to pass value as None
    builder.append(
        0, timestamp=9999999, key=b"123", value=None, headers=[])

    # Timestamp can be None
    builder.append(
        1, timestamp=None, key=None, value=b"some string", headers=[])

    # Ok to pass offsets in not incremental order. This should not happen thou
    builder.append(
        5, timestamp=9999999, key=b"123", value=None, headers=[])

    # Check record with headers
    builder.append(
        6, timestamp=9999999, key=b"234", value=None, headers=[("hkey", b"hval")])

    # in case error handling code fails to fix inner buffer in builder
    assert len(builder.build()) == 124


def test_default_correct_metadata_response():
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024 * 1024)
    meta = builder.append(
        0, timestamp=9999999, key=b"test", value=b"Super", headers=[])

    assert meta.offset == 0
    assert meta.timestamp == 9999999
    assert meta.crc is None
    assert meta.size == 16
    assert repr(meta) == (
        "DefaultRecordMetadata(offset=0, size={}, timestamp={})"
        .format(meta.size, meta.timestamp)
    )


def test_default_batch_size_limit():
    # First message can be added even if it's too big
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024)

    meta = builder.append(
        0, timestamp=None, key=None, value=b"M" * 2000, headers=[])
    assert meta.size > 0
    assert meta.crc is None
    assert meta.offset == 0
    assert meta.timestamp is not None
    assert len(builder.build()) > 2000

    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024)
    meta = builder.append(
        0, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is not None
    meta = builder.append(
        1, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is None
    meta = builder.append(
        2, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is None
    assert len(builder.build()) < 1000


@pytest.mark.parametrize("compression_type,name,checker_name", [
    (DefaultRecordBatch.CODEC_GZIP, "gzip", "has_gzip"),
    (DefaultRecordBatch.CODEC_SNAPPY, "snappy", "has_snappy"),
    (DefaultRecordBatch.CODEC_LZ4, "lz4", "has_lz4")
])
@pytest.mark.parametrize("magic", [0, 1])
def test_unavailable_codec(magic, compression_type, name, checker_name):
    if not getattr(kafka.codec, checker_name)():
        pytest.skip('%s compression_type not installed' % (compression_type,))
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024)
    builder.append(0, timestamp=None, key=None, value=b"M" * 2000, headers=[])
    correct_buffer = builder.build()

    with patch.object(kafka.codec, checker_name) as mocked:
        mocked.return_value = False
        # Check that builder raises error
        builder = DefaultRecordBatchBuilder(
            magic=2, compression_type=compression_type, is_transactional=0,
            producer_id=-1, producer_epoch=-1, base_sequence=-1,
            batch_size=1024)
        error_msg = "Libraries for {} compression codec not found".format(name)
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            builder.append(0, timestamp=None, key=None, value=b"M", headers=[])
            builder.build()

        # Check that reader raises same error
        batch = DefaultRecordBatch(bytes(correct_buffer))
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            list(batch)


def _encode_varint_bytes(value):
    out = bytearray()
    encode_varint(value, out.append)
    return bytes(out)


def _encode_nullable_bytes(value):
    if value is None:
        return _encode_varint_bytes(-1)
    return _encode_varint_bytes(len(value)) + bytes(value)


def _valid_record_parts(key=b"key", value=b"value", headers=(),
                        timestamp_delta=0, offset_delta=0):
    parts = [
        b"\x00",  # attributes
        _encode_varint_bytes(timestamp_delta),
        _encode_varint_bytes(offset_delta),
        _encode_nullable_bytes(key),
        _encode_nullable_bytes(value),
        _encode_varint_bytes(len(headers)),
    ]
    for h_key, h_value in headers:
        h_key_bytes = h_key.encode("utf-8")
        parts.append(_encode_varint_bytes(len(h_key_bytes)) + h_key_bytes)
        parts.append(_encode_nullable_bytes(h_value))
    return parts


def _record_from_parts(parts, length=None):
    body = b"".join(parts)
    if length is None:
        length = len(body)
    return _encode_varint_bytes(length) + body


def _encode_record(**kwargs):
    return _record_from_parts(_valid_record_parts(**kwargs))


def _make_batch(records, compression_type=DefaultRecordBatch.CODEC_NONE,
                base_offset=5, first_timestamp=1000):
    # Assemble a batch with a freshly computed CRC, so tests prove that the
    # record structure checks (not the CRC) reject malformed input.
    payload = b"".join(records)
    if compression_type != DefaultRecordBatch.CODEC_NONE:
        payload = gzip_encode(payload)
    buf = bytearray(DefaultRecordBatch.HEADER_STRUCT.size) + bytearray(payload)
    DefaultRecordBatch.HEADER_STRUCT.pack_into(
        buf, 0,
        base_offset,
        len(buf) - DefaultRecordBatch.AFTER_LEN_OFFSET,
        0,  # PartitionLeaderEpoch
        2,  # Magic
        0,  # CRC, set below
        compression_type,
        len(records) - 1,  # LastOffsetDelta
        first_timestamp,
        first_timestamp,
        -1, -1, -1,  # ProducerId, ProducerEpoch, BaseSequence
        len(records))
    crc = calc_crc32c(memoryview(buf)[DefaultRecordBatch.ATTRIBUTES_OFFSET:])
    struct.pack_into(">I", buf, DefaultRecordBatch.CRC_OFFSET, crc)
    return bytes(buf)


def _malformed_record_cases():
    valid = _valid_record_parts(headers=[("hk", b"hv")])
    body_len = len(b"".join(valid))
    cases = {}

    # Record length field
    cases["negative_record_length"] = _record_from_parts(valid, length=-1)
    cases["record_length_too_small"] = _record_from_parts(
        valid, length=body_len - 1)
    cases["record_length_too_large"] = _record_from_parts(
        valid, length=body_len + 1)

    # Key length field
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_varint_bytes(100) + b"key",
             _encode_nullable_bytes(b"value"), b"\x00"]
    cases["key_length_too_large"] = _record_from_parts(parts)
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_varint_bytes(-2) + b"key",
             _encode_nullable_bytes(b"value"), b"\x00"]
    cases["key_length_invalid_negative"] = _record_from_parts(parts)

    # Value length field
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"),
             _encode_varint_bytes(100) + b"value", b"\x00"]
    cases["value_length_too_large"] = _record_from_parts(parts)
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"),
             _encode_varint_bytes(-2) + b"value", b"\x00"]
    cases["value_length_invalid_negative"] = _record_from_parts(parts)

    # Header count field
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"), _encode_nullable_bytes(b"value"),
             _encode_varint_bytes(-1)]
    cases["negative_header_count"] = _record_from_parts(parts)
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"), _encode_nullable_bytes(b"value"),
             _encode_varint_bytes(2),
             _encode_varint_bytes(2) + b"hk", _encode_nullable_bytes(b"hv")]
    cases["header_count_too_large"] = _record_from_parts(parts)

    # Header key length field
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"), _encode_nullable_bytes(b"value"),
             _encode_varint_bytes(1),
             _encode_varint_bytes(100) + b"hk", _encode_nullable_bytes(b"hv")]
    cases["header_key_length_too_large"] = _record_from_parts(parts)
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"), _encode_nullable_bytes(b"value"),
             _encode_varint_bytes(1),
             _encode_varint_bytes(-1) + b"hk", _encode_nullable_bytes(b"hv")]
    cases["header_key_length_negative"] = _record_from_parts(parts)

    # Header key that is truncated utf-8 (0xC3 starts a 2-byte sequence)
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"), _encode_nullable_bytes(b"value"),
             _encode_varint_bytes(1),
             _encode_varint_bytes(1) + b"\xc3", _encode_nullable_bytes(b"hv")]
    cases["header_key_truncated_utf8"] = _record_from_parts(parts)

    # Header value length field
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"), _encode_nullable_bytes(b"value"),
             _encode_varint_bytes(1),
             _encode_varint_bytes(2) + b"hk",
             _encode_varint_bytes(100) + b"hv"]
    cases["header_value_length_too_large"] = _record_from_parts(parts)
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_nullable_bytes(b"key"), _encode_nullable_bytes(b"value"),
             _encode_varint_bytes(1),
             _encode_varint_bytes(2) + b"hk",
             _encode_varint_bytes(-2) + b"hv"]
    cases["header_value_length_invalid_negative"] = _record_from_parts(parts)

    # Varint that runs past the record boundary mid-sequence
    parts = [b"\x00", b"\x00", b"\x00", b"\x82"]
    cases["truncated_varint_at_record_end"] = _record_from_parts(parts)

    # Varint that exceeds the int64 range (11 bytes)
    parts = [b"\x00", b"\x00", b"\x00",
             b"\x80" * 10 + b"\x01",
             _encode_nullable_bytes(b"value"), b"\x00"]
    cases["varint_out_of_int64_range"] = _record_from_parts(parts)

    return cases


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
])
@pytest.mark.parametrize("case", sorted(_malformed_record_cases()))
def test_malformed_record_rejected_despite_valid_crc(case, compression_type):
    malformed = _malformed_record_cases()[case]
    second = _encode_record(key=b"record2-key", value=b"record2-value")
    buffer = _make_batch([malformed, second],
                         compression_type=compression_type)
    batch = DefaultRecordBatch(buffer)
    # The CRC is recomputed over the malformed payload, so only structural
    # validation can reject this batch.
    assert batch.validate_crc() is True
    with pytest.raises(CorruptRecordError,
                       match="base_offset=5.*record index 0"):
        list(batch)


def test_oversized_key_length_does_not_swallow_next_record():
    # A key length that would extend into the following record must fail
    # instead of returning record 2's bytes as record 1's key.
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_varint_bytes(100) + b"key",
             _encode_nullable_bytes(b"value"), b"\x00"]
    first = _record_from_parts(parts)
    second = _encode_record(key=b"record2-key", value=b"record2-value")
    batch = DefaultRecordBatch(_make_batch([first, second]))
    with pytest.raises(CorruptRecordError):
        list(batch)

    # Same payload with a corrected (small) key length parses fine and does
    # not contain any of record 2's bytes.
    parts[3] = _encode_varint_bytes(3) + b"key"
    batch = DefaultRecordBatch(_make_batch([_record_from_parts(parts), second]))
    records = list(batch)
    assert records[0].key == b"key"
    assert records[1].key == b"record2-key"


def test_valid_record_then_malformed_record():
    first = _encode_record(key=b"record1-key", value=b"record1-value")
    parts = [b"\x00", b"\x00", b"\x00",
             _encode_varint_bytes(100) + b"key",
             _encode_nullable_bytes(b"value"), b"\x00"]
    second = _record_from_parts(parts)
    batch = DefaultRecordBatch(_make_batch([first, second]))
    it = iter(batch)
    record = next(it)
    assert record.key == b"record1-key"
    assert record.value == b"record1-value"
    with pytest.raises(CorruptRecordError,
                       match="base_offset=5.*record index 1"):
        next(it)


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
])
def test_record_edge_cases_still_readable(compression_type):
    big_key = b"k" * 70000  # 3-byte length varint
    headers = [("dup", b"1"), ("dup", b"2"), ("null-value", None)]
    records = [
        _encode_record(key=None, value=None, headers=[]),
        _encode_record(key=b"", value=b"", headers=[]),
        _encode_record(key=b"key", value=b"value", headers=headers),
        _encode_record(key=big_key, value=b"value",
                       timestamp_delta=2 ** 63 - 1, offset_delta=2 ** 31),
    ]
    batch = DefaultRecordBatch(
        _make_batch(records, compression_type=compression_type))
    assert batch.validate_crc() is True
    parsed = list(batch)
    assert len(parsed) == 4

    assert parsed[0].key is None
    assert parsed[0].value is None
    assert parsed[0].headers == []

    assert parsed[1].key == b""
    assert parsed[1].value == b""

    assert parsed[2].headers == [("dup", b"1"), ("dup", b"2"),
                                 ("null-value", None)]

    assert parsed[3].key == big_key
    assert parsed[3].timestamp == 1000 + 2 ** 63 - 1
    assert parsed[3].offset == 5 + 2 ** 31
