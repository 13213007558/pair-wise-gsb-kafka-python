# pylint: skip-file
"""Synthetic Fetch-response tests for read_committed (KIP-98) consumption.

These tests build transactional record batches -- including abort / commit
control batches -- by hand, feed them to the Fetcher as CompletedFetch
responses, and record every raw key/value the business deserializers see.
Aborted-transaction payloads and control records must never reach them, and
the consumed position must keep advancing so the consumer neither loops on
skipped transactions nor skips later legitimate records.
"""
import struct

import pytest

from unittest.mock import MagicMock

from kafka.consumer.fetcher import CompletedFetch, Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.protocol.consumer import FetchResponse
from kafka.record.default_records import ControlRecord, DefaultRecordBatch
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.record.util import calc_crc32c
from kafka.serializer import Deserializer
from kafka.structs import TopicPartition


_ResponsePartition = FetchResponse.FetchableTopicResponse.PartitionData
_AbortedTransaction = _ResponsePartition.AbortedTransaction

PRODUCER_A = 1001
PRODUCER_B = 2002
_TIMESTAMP = 1700000000000


class RecordingDeserializer(Deserializer):
    """Business deserializer stand-in: logs every raw payload it is given."""

    def __init__(self):
        self.received = []

    def deserialize(self, topic, headers, data):
        self.received.append(data)
        return data


@pytest.fixture
def subscription_state():
    return SubscriptionState()


@pytest.fixture
def tp():
    return TopicPartition('foobar', 0)


@pytest.fixture
def key_deserializer():
    return RecordingDeserializer()


@pytest.fixture
def value_deserializer():
    return RecordingDeserializer()


@pytest.fixture
def fetcher(client, metrics, subscription_state, tp, key_deserializer, value_deserializer):
    subscription_state.subscribe(topics=[tp.topic])
    subscription_state.assign_from_subscribed([tp])
    subscription_state.seek(tp, 0)
    return Fetcher(client, subscription_state,
                   metrics=metrics,
                   isolation_level='read_committed',
                   key_deserializer=key_deserializer,
                   value_deserializer=value_deserializer)


def _data_batch(records, offset, producer_id=None, base_sequence=0):
    """A magic=2 data batch; transactional when producer_id is given."""
    kwargs = {}
    if producer_id is not None:
        kwargs = dict(transactional=True, producer_id=producer_id,
                      producer_epoch=0, base_sequence=base_sequence)
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999, offset=offset, **kwargs)
    for key, value in records:
        builder.append(timestamp=_TIMESTAMP, key=key, value=value, headers=[])
    builder.close()
    return builder.buffer()


def _control_batch(offset, producer_id, abort, base_sequence=0):
    """A transaction marker (control) batch, as written by the broker's
    EndTxn handling: single record, control attribute bit set."""
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999, offset=offset,
        transactional=True, producer_id=producer_id,
        producer_epoch=0, base_sequence=base_sequence)
    key = ControlRecord.KEY_STRUCT.pack(0, 0 if abort else 1)
    value = struct.pack('>hi', 0, 0)  # version + coordinator epoch
    builder.append(timestamp=_TIMESTAMP, key=key, value=value, headers=[])
    builder.close()
    buf = bytearray(builder.buffer())
    attrs, = struct.unpack_from('>h', buf, DefaultRecordBatch.ATTRIBUTES_OFFSET)
    struct.pack_into('>h', buf, DefaultRecordBatch.ATTRIBUTES_OFFSET,
                     attrs | DefaultRecordBatch.CONTROL_MASK)
    crc = calc_crc32c(memoryview(buf)[DefaultRecordBatch.ATTRIBUTES_OFFSET:])
    struct.pack_into('>I', buf, DefaultRecordBatch.CRC_OFFSET, crc)
    return bytes(buf)


def _completed_fetch(tp, batches, fetch_offset, aborted=None, highwater=100):
    partition_data = _ResponsePartition(
        error_code=0,
        high_watermark=highwater,
        last_stable_offset=highwater,
        aborted_transactions=[
            _AbortedTransaction(producer_id=pid, first_offset=first_offset)
            for pid, first_offset in (aborted or [])
        ],
        records=b''.join(batches),
        version=4)
    return CompletedFetch(tp, fetch_offset, 4, partition_data, MagicMock())


def _position(subscription_state, tp):
    return subscription_state.position(tp).offset


def test_abort_marker_arriving_in_later_fetch_response(
        fetcher, subscription_state, tp, key_deserializer, value_deserializer):
    # First response carries only the aborted data batch; the abort marker
    # itself has not been fetched yet. The aborted_transactions listing in
    # the response is what allows filtering here.
    fetcher._completed_fetches.append(_completed_fetch(
        tp,
        [_data_batch([(b'k0', b'A-aborted-0'), (b'k1', b'A-aborted-1')],
                     offset=0, producer_id=PRODUCER_A)],
        fetch_offset=0,
        aborted=[(PRODUCER_A, 0)]))
    records, partial = fetcher.fetched_records()
    assert records == {}
    assert partial is False
    # Position moved past the skipped batch instead of stalling on it.
    assert _position(subscription_state, tp) == 2
    assert key_deserializer.received == []
    assert value_deserializer.received == []

    # Second response: the abort control batch (offset 2), followed by a
    # committed transaction from the same producer (offsets 3-4).
    fetcher._completed_fetches.append(_completed_fetch(
        tp,
        [_control_batch(offset=2, producer_id=PRODUCER_A, abort=True),
         _data_batch([(b'k3', b'A-committed-3'), (b'k4', b'A-committed-4')],
                     offset=3, producer_id=PRODUCER_A, base_sequence=2)],
        fetch_offset=2))
    records, partial = fetcher.fetched_records()
    assert [r.offset for r in records[tp]] == [3, 4]
    assert [r.value for r in records[tp]] == [b'A-committed-3', b'A-committed-4']
    assert _position(subscription_state, tp) == 5
    # The control record and the aborted payloads never reached the
    # business deserializers.
    assert key_deserializer.received == [b'k3', b'k4']
    assert value_deserializer.received == [b'A-committed-3', b'A-committed-4']


def test_committed_transaction_after_abort_same_producer(
        fetcher, subscription_state, tp, key_deserializer, value_deserializer):
    # Aborted txn (0-1), its abort marker (2), then a committed txn (3-4)
    # from the same producer, all in one response. The abort marker must
    # reset the producer's aborted state so the committed txn is delivered.
    fetcher._completed_fetches.append(_completed_fetch(
        tp,
        [_data_batch([(b'k0', b'A-aborted-0'), (b'k1', b'A-aborted-1')],
                     offset=0, producer_id=PRODUCER_A),
         _control_batch(offset=2, producer_id=PRODUCER_A, abort=True),
         _data_batch([(b'k3', b'A-committed-3'), (b'k4', b'A-committed-4')],
                     offset=3, producer_id=PRODUCER_A, base_sequence=2)],
        fetch_offset=0,
        aborted=[(PRODUCER_A, 0)]))
    records, _ = fetcher.fetched_records()
    assert [r.offset for r in records[tp]] == [3, 4]
    assert [r.value for r in records[tp]] == [b'A-committed-3', b'A-committed-4']
    assert _position(subscription_state, tp) == 5
    assert key_deserializer.received == [b'k3', b'k4']
    assert value_deserializer.received == [b'A-committed-3', b'A-committed-4']


def test_interleaved_producers(
        fetcher, subscription_state, tp, key_deserializer, value_deserializer):
    # Producer A aborts its first txn; producer B commits; A then commits a
    # second txn. Only B's records and A's second txn may be delivered.
    fetcher._completed_fetches.append(_completed_fetch(
        tp,
        [_data_batch([(b'a0', b'A-aborted-0'), (b'a1', b'A-aborted-1')],
                     offset=0, producer_id=PRODUCER_A),
         _data_batch([(b'b2', b'B-committed-2'), (b'b3', b'B-committed-3')],
                     offset=2, producer_id=PRODUCER_B),
         _control_batch(offset=4, producer_id=PRODUCER_A, abort=True, base_sequence=2),
         _control_batch(offset=5, producer_id=PRODUCER_B, abort=False, base_sequence=2),
         _data_batch([(b'a6', b'A-committed-6'), (b'a7', b'A-committed-7')],
                     offset=6, producer_id=PRODUCER_A, base_sequence=3)],
        fetch_offset=0,
        aborted=[(PRODUCER_A, 0)]))
    records, _ = fetcher.fetched_records()
    assert [r.offset for r in records[tp]] == [2, 3, 6, 7]
    assert [r.value for r in records[tp]] == [
        b'B-committed-2', b'B-committed-3', b'A-committed-6', b'A-committed-7']
    assert _position(subscription_state, tp) == 8
    assert key_deserializer.received == [b'b2', b'b3', b'a6', b'a7']
    assert value_deserializer.received == [
        b'B-committed-2', b'B-committed-3', b'A-committed-6', b'A-committed-7']


def test_max_poll_records_truncates_mid_batch(
        fetcher, subscription_state, tp, key_deserializer, value_deserializer):
    # An aborted txn (0-1) + marker (2), then a 5-record committed batch
    # (3-7). max_records slices the committed batch; each poll must resume
    # exactly where the previous one stopped.
    committed = [(b'k%d' % i, b'committed-%d' % i) for i in range(3, 8)]
    fetcher._completed_fetches.append(_completed_fetch(
        tp,
        [_data_batch([(b'k0', b'aborted-0'), (b'k1', b'aborted-1')],
                     offset=0, producer_id=PRODUCER_A),
         _control_batch(offset=2, producer_id=PRODUCER_A, abort=True),
         _data_batch(committed, offset=3, producer_id=PRODUCER_A, base_sequence=2)],
        fetch_offset=0,
        aborted=[(PRODUCER_A, 0)]))

    records, _ = fetcher.fetched_records(max_records=2)
    assert [r.offset for r in records[tp]] == [3, 4]
    assert _position(subscription_state, tp) == 5

    records, _ = fetcher.fetched_records(max_records=2)
    assert [r.offset for r in records[tp]] == [5, 6]
    assert _position(subscription_state, tp) == 7

    records, _ = fetcher.fetched_records(max_records=2)
    assert [r.offset for r in records[tp]] == [7]
    assert _position(subscription_state, tp) == 8

    # Nothing left: the call returns promptly without looping or re-reading.
    records, partial = fetcher.fetched_records(max_records=2)
    assert records == {}
    assert partial is False
    assert _position(subscription_state, tp) == 8

    # Each committed payload deserialized exactly once, in order; the
    # aborted payloads and the control record never appeared.
    assert key_deserializer.received == [b'k%d' % i for i in range(3, 8)]
    assert value_deserializer.received == [b'committed-%d' % i for i in range(3, 8)]


def test_seek_and_reread(
        fetcher, subscription_state, tp, key_deserializer, value_deserializer):
    batches = [
        _data_batch([(b'k0', b'A-aborted-0'), (b'k1', b'A-aborted-1')],
                    offset=0, producer_id=PRODUCER_A),
        _control_batch(offset=2, producer_id=PRODUCER_A, abort=True),
        _data_batch([(b'k3', b'A-committed-3'), (b'k4', b'A-committed-4')],
                    offset=3, producer_id=PRODUCER_A, base_sequence=2),
    ]
    fetcher._completed_fetches.append(
        _completed_fetch(tp, batches, fetch_offset=0, aborted=[(PRODUCER_A, 0)]))
    records, _ = fetcher.fetched_records()
    assert [r.offset for r in records[tp]] == [3, 4]
    assert _position(subscription_state, tp) == 5

    # Seek back and re-fetch the same log segment: read_committed filtering
    # must produce the same result a second time.
    subscription_state.seek(tp, 0)
    fetcher._completed_fetches.append(
        _completed_fetch(tp, batches, fetch_offset=0, aborted=[(PRODUCER_A, 0)]))
    records, _ = fetcher.fetched_records()
    assert [r.offset for r in records[tp]] == [3, 4]
    assert [r.value for r in records[tp]] == [b'A-committed-3', b'A-committed-4']
    assert _position(subscription_state, tp) == 5

    # Committed payloads were deserialized once per pass; aborted and
    # control-record payloads never reached the deserializers.
    assert key_deserializer.received == [b'k3', b'k4', b'k3', b'k4']
    assert value_deserializer.received == [
        b'A-committed-3', b'A-committed-4', b'A-committed-3', b'A-committed-4']
