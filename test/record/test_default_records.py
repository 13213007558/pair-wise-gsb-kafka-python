# -*- coding: utf-8 -*-

import struct
from unittest.mock import patch

import pytest

import kafka.codec
from kafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder
)
from kafka.errors import CorruptRecordError, UnsupportedCodecError
from kafka.record.util import calc_crc32c, encode_varint
from kafka.codec import gzip_decode, gzip_encode

from test.testutil import maybe_skip_unsupported_compression


HEADER = DefaultRecordBatch.HEADER_STRUCT


def encoded_varint(value):
    result = bytearray()
    encode_varint(value, result.append)
    return bytes(result)


def build_record(key, value, headers, offset_delta=0, timestamp_delta=0,
                 attributes=0):
    body = bytearray([attributes])
    body.extend(encoded_varint(timestamp_delta))
    body.extend(encoded_varint(offset_delta))

    if key is None:
        body.append(1)
    else:
        body.extend(encoded_varint(len(key)))
        body.extend(key)

    if value is None:
        body.append(1)
    else:
        body.extend(encoded_varint(len(value)))
        body.extend(value)

    body.extend(encoded_varint(len(headers)))
    for header_key, header_value in headers:
        encoded_key = header_key.encode("utf-8")
        body.extend(encoded_varint(len(encoded_key)))
        body.extend(encoded_key)
        if header_value is None:
            body.append(1)
        else:
            body.extend(encoded_varint(len(header_value)))
            body.extend(header_value)

    record = bytearray(encoded_varint(len(body)))
    record.extend(body)
    return bytes(record)


def recalculate_crc(batch):
    batch = bytearray(batch)
    fields = list(HEADER.unpack_from(batch))
    fields[1] = len(batch) - DefaultRecordBatch.AFTER_LEN_OFFSET
    fields[4] = 0
    HEADER.pack_into(batch, 0, *fields)
    crc = calc_crc32c(
        memoryview(batch)[DefaultRecordBatch.ATTRIBUTES_OFFSET:])
    struct.pack_into(">I", batch, DefaultRecordBatch.CRC_OFFSET, crc)
    return bytes(batch)


def build_raw_batch(payload, records_count, base_offset=1234,
                    last_offset_delta=1, compression_type=0):
    batch = bytearray(HEADER.size + len(payload))
    batch[HEADER.size:] = payload
    HEADER.pack_into(
        batch, 0,
        base_offset,
        len(batch) - DefaultRecordBatch.AFTER_LEN_OFFSET,
        0, 2, 0, compression_type, last_offset_delta,
        0, 0, DefaultRecordBatch.NO_PRODUCER_ID,
        DefaultRecordBatch.NO_PRODUCER_ID,
        DefaultRecordBatch.NO_SEQUENCE, records_count)
    return recalculate_crc(batch)


def build_batch(records, records_count=None, **kwargs):
    return build_raw_batch(
        b"".join(records),
        len(records) if records_count is None else records_count,
        **kwargs)


def sample_records():
    return (
        build_record(b"K", b"V", [("H", b"X")]),
        build_record(b"k", b"NEXT", [], offset_delta=1)
    )


def assert_valid_crc_but_corrupt(batch, message):
    reader = DefaultRecordBatch(batch)
    assert reader.validate_crc() is True
    with pytest.raises(CorruptRecordError, match=message):
        list(reader)


def replace_byte(batch, index, value):
    corrupted = bytearray(batch)
    corrupted[index] = value
    return recalculate_crc(corrupted)


def replace_bytes(batch, index, value):
    corrupted = bytearray(batch)
    corrupted[index:index + 1] = value
    return recalculate_crc(corrupted)


@pytest.mark.parametrize("relative_position,value,message", [
    (0, 0x01, "negative record length -1"),
    (4, 0x03, "invalid nullable key length -2"),
    (6, 0x03, "invalid nullable value length -2"),
    (8, 0x01, "invalid record header count -1"),
    (9, 0x01, "negative header key length -1"),
    (11, 0x03, "invalid nullable header value length -2"),
])
def test_invalid_negative_lengths(relative_position, value, message):
    batch = build_batch(list(sample_records()))
    corrupted = replace_byte(
        batch, HEADER.size + relative_position, value)
    assert_valid_crc_but_corrupt(corrupted, message)


@pytest.mark.parametrize("relative_position,value,message", [
    (0, encoded_varint(127), "record length 127 extends beyond batch boundary"),
    (4, encoded_varint(16),
     "key length 16 extends beyond record boundary.*8 bytes remaining"),
    (6, encoded_varint(15),
     "value length 15 extends beyond record boundary.*6 bytes remaining"),
    (9, encoded_varint(4),
     "header key length 4 extends beyond record boundary.*3 bytes "
     "remaining"),
    (11, encoded_varint(3),
     "header value length 3 extends beyond record boundary.*1 bytes "
     "remaining"),
])
def test_length_fields_cannot_cross_record_boundary(
        relative_position, value, message):
    batch = build_batch(list(sample_records()))
    corrupted = replace_bytes(
        batch, HEADER.size + relative_position, value)
    assert_valid_crc_but_corrupt(
        corrupted,
        "batch base offset 1234, record index 0.*" + message)


@pytest.mark.parametrize("relative_position,value,max_bytes,message", [
    (0, b"\x80" * 6, 5, "record length varint is longer than 5 bytes"),
    (1, b"\x80" * 11, 10, "timestamp delta varint is longer than 10 bytes"),
    (4, b"\x80" * 6, 5, "key length varint is longer than 5 bytes"),
])
def test_overlong_varints_are_confined_to_record(
        relative_position, value, max_bytes, message):
    del max_bytes
    batch = build_batch(list(sample_records()))
    corrupted = replace_bytes(
        batch, HEADER.size + relative_position, value)
    assert_valid_crc_but_corrupt(corrupted, message)


def test_truncated_varint_ends_at_record_boundary():
    record_body = bytes([0, 0, 0, 0x80])
    record = encoded_varint(len(record_body)) + record_body
    batch = build_raw_batch(record, 1)
    assert_valid_crc_but_corrupt(
        batch,
        r"batch base offset 1234, record index 0.*truncated key length varint "
        r"at byte 4")


def test_truncated_utf8_header_key_is_corrupt_record():
    record_body = bytes([
        0, 0, 0,             # attributes, timestamp delta, offset delta
        1,                   # null key
        1,                   # null value
        2,                   # one header
        2,                   # header key length of one
        0xff                 # invalid UTF-8
    ])
    record = encoded_varint(len(record_body)) + record_body
    batch = build_raw_batch(record, 1)
    assert_valid_crc_but_corrupt(
        batch,
        r"record offset 1234: invalid UTF-8 header key")


def test_declared_header_count_is_confined_to_record():
    record_body = bytes([
        0, 0, 0,  # attributes, timestamp delta, offset delta
        1, 1,     # null key and null value
        2         # declares a header, but no header bytes follow
    ])
    record = encoded_varint(len(record_body)) + record_body
    batch = build_raw_batch(record, 1)
    assert_valid_crc_but_corrupt(
        batch,
        r"truncated header key length varint at byte 6")


def test_declared_header_count_must_consume_record():
    batch = build_batch(list(sample_records()))
    corrupted = replace_byte(batch, HEADER.size + 8, 0)
    assert_valid_crc_but_corrupt(
        corrupted,
        r"record offset 1234: invalid record size: declared 12 payload "
        r"bytes, consumed 8")


def test_declared_batch_record_count_cannot_read_past_batch():
    batch = bytearray(build_batch(list(sample_records()), records_count=3,
                                  last_offset_delta=2))
    corrupted = recalculate_crc(batch)
    assert_valid_crc_but_corrupt(
        corrupted,
        r"batch base offset 1234, record index 2: truncated record length "
        r"varint at byte")


def test_declared_batch_record_count_cannot_leave_records():
    corrupted = build_batch(
        list(sample_records()), records_count=1, last_offset_delta=0)
    assert_valid_crc_but_corrupt(
        corrupted,
        r"batch base offset 1234: 12 unconsumed bytes after all records "
        r"consumed")


def test_compressed_batch_uses_same_record_boundaries():
    maybe_skip_unsupported_compression(DefaultRecordBatch.CODEC_GZIP)
    payload = b"".join(sample_records())
    corrupted_payload = bytearray(payload)
    corrupted_payload[4:5] = encoded_varint(16)
    compressed = gzip_encode(bytes(corrupted_payload))
    batch = build_raw_batch(
        compressed, 2,
        compression_type=DefaultRecordBatch.CODEC_GZIP)

    assert calc_crc32c(memoryview(batch)[DefaultRecordBatch.ATTRIBUTES_OFFSET:]) \
        == DefaultRecordBatch(batch).crc
    assert gzip_decode(memoryview(batch)[HEADER.size:]) == bytes(corrupted_payload)
    assert_valid_crc_but_corrupt(
        batch, "key length 16 extends beyond record boundary")


def test_valid_null_empty_duplicate_and_max_varint_records():
    max_int32 = DefaultRecordBatch.MAX_INT
    max_int64 = (1 << 63) - 1
    first = build_record(
        None, b"", [("duplicate", b"x"), ("duplicate", b"x")])
    second = build_record(
        b"", None,
        [("", b""), ("", None)],
        offset_delta=max_int32, timestamp_delta=max_int64)
    batch = build_batch(
        [first, second], base_offset=0, last_offset_delta=max_int32)

    reader = DefaultRecordBatch(batch)
    assert reader.validate_crc() is True
    records = list(reader)

    assert records[0].key is None
    assert records[0].value == b""
    assert records[0].headers == [
        ("duplicate", b"x"), ("duplicate", b"x")]
    assert records[1].key == b""
    assert records[1].value is None
    assert records[1].offset == max_int32
    assert records[1].timestamp == max_int64
    assert records[1].headers == [("", b""), ("", None)]


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
