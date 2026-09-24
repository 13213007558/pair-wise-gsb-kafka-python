"""End-to-end tests for the KIP-480 sticky partitioner.

These tests drive a real KafkaProducer + Sender against a MockBroker and
assert on the actual ProduceRequests the broker receives - batch counts,
the partition each batch was assigned to, and per-partition record
ordering. Calling StickyPartitioner.partition() in isolation and
collecting the returned list would not prove the RecordAccumulator-driven
rotation is wired up, so everything here is verified on the wire.
"""

import collections
import random

import kafka.errors as Errors
from kafka import KafkaProducer
from kafka.partitioner import StickyPartitioner
from kafka.protocol.metadata import MetadataResponse
from kafka.protocol.producer import ProduceRequest, ProduceResponse
from kafka.record.memory_records import MemoryRecords, MemoryRecordsBuilder

from test.mock_broker import MockBroker


_TOPIC = 'sticky-e2e'
_NUM_PARTITIONS = 4
# Fixed timestamp for every record so serialized record sizes (and hence
# batch capacities) are deterministic.
_TS = 1700000000000
_PER_BATCH = 4


def _value(n):
    """Fixed-width, self-identifying record value (8 bytes for n < 10000)."""
    return b'msg-%04d' % n


def _metadata_topic(num_partitions, version=8):
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    return Topic(
        version=version, error_code=0, name=_TOPIC, is_internal=False,
        partitions=[
            Partition(version=version, error_code=0, partition_index=i,
                      leader_id=0, leader_epoch=0, replica_nodes=[0],
                      isr_nodes=[0], offline_replicas=[])
            for i in range(num_partitions)
        ])


def _batch_size_for(count):
    """The batch_size that fits exactly ``count`` records and is then full.

    Probed with the real builder so the test doesn't hardcode record
    overhead: after ``count`` appends the builder size is exactly the
    batch_size at which is_full() flips True and the next append fails.
    """
    builder = MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=2**20)
    for i in range(count):
        metadata = builder.append(timestamp=_TS, key=None, value=_value(i), headers=[])
        assert metadata is not None
    batch_size = builder.size_in_bytes()
    # Sanity: one more record of the same shape must not fit.
    probe = MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=batch_size)
    for i in range(count):
        metadata = probe.append(timestamp=_TS, key=None, value=_value(i), headers=[])
        assert metadata is not None
    assert probe.append(timestamp=_TS, key=None, value=_value(count), headers=[]) is None
    assert probe.is_full()
    return batch_size


def _produce_response(topic_partitions):
    """Build a ProduceResponse. ``topic_partitions`` is a list of
    (topic_name, [(partition_index, error_code, base_offset), ...])."""
    Topic = ProduceResponse.TopicProduceResponse
    Partition = Topic.PartitionProduceResponse
    return ProduceResponse(
        throttle_time_ms=0,
        responses=[
            Topic(name=name, partition_responses=[
                Partition(index=index, error_code=error_code,
                          base_offset=base_offset, log_append_time_ms=-1,
                          log_start_offset=0, record_errors=[],
                          error_message=None, current_leader=None)
                for index, error_code, base_offset in partitions
            ])
            for name, partitions in topic_partitions
        ])


class _ProduceCapture:
    """MockBroker respond_always handler for ProduceRequest.

    Decodes every ProduceRequest the sender issues, appends one
    (topic, partition, [values]) entry per contained record batch to
    the batches list (in arrival order), and replies with a success
    response carrying sane per-partition base offsets. When
    fail_first_errno is set, the very first request is instead
    answered with that (retriable) error for all of its partitions.
    """

    def __init__(self, fail_first_errno=None):
        self.batches = []
        self._offsets = collections.Counter()
        self._fail_first_errno = fail_first_errno
        self._requests_seen = 0

    def __call__(self, api_key, api_version, correlation_id, request_bytes):
        self._requests_seen += 1
        fail = (self._fail_first_errno is not None
                and self._requests_seen == 1)
        request = ProduceRequest.decode(request_bytes, version=api_version, header=True)
        topic_partitions = []
        for topic_data in request.topic_data:
            partition_responses = []
            for partition_data in topic_data.partition_data:
                tp = (topic_data.name, partition_data.index)
                values = []
                for record_batch in MemoryRecords(bytes(partition_data.records)):
                    values.extend(record.value for record in record_batch)
                self.batches.append((topic_data.name, partition_data.index, values))
                if fail:
                    partition_responses.append(
                        (partition_data.index, self._fail_first_errno, -1))
                else:
                    base_offset = self._offsets[tp]
                    self._offsets[tp] += len(values)
                    partition_responses.append(
                        (partition_data.index, 0, base_offset))
            topic_partitions.append((topic_data.name, partition_responses))
        return _produce_response(topic_partitions)


def _expected_batch_partitions(seed, num_partitions, num_batches):
    """Replay the sticky partitioner picks against a fixed random source.

    The first send draws the initial sticky partition; every subsequent
    batch is preceded by an abort-driven rotation that draws again,
    avoiding the previous partition. Batch i therefore lands on
    draw i + 1.
    """
    rng = random.Random(seed)
    # Mirror _pick_sticky_locked: candidates come from
    # list(available_partitions_for_topic(...)), a set of ints.
    available = list(set(range(num_partitions)))
    picks = [rng.choice(available)]
    for _ in range(num_batches):
        candidates = [p for p in available if p != picks[-1]]
        picks.append(rng.choice(candidates))
    return picks[1:]


def _make_producer(broker, monkeypatch, seed, **configs):
    """A real KafkaProducer + Sender against broker, with the sticky
    partitioner's random source pinned to a seeded Random instance.
    """
    if seed is not None:
        monkeypatch.setattr('kafka.partitioner.sticky.random', random.Random(seed))
    configs.setdefault('api_version', (2, 5))
    configs.setdefault('enable_idempotence', False)
    configs.setdefault('partitioner', StickyPartitioner())
    configs.setdefault('batch_size', _batch_size_for(_PER_BATCH))
    # High linger: batches are sent when full (or on flush), never early,
    # which keeps batch boundaries deterministic.
    configs.setdefault('linger_ms', 60000)
    return KafkaProducer(
        kafka_client=broker.client_factory(),
        bootstrap_servers=['%s:%d' % (broker.host, broker.port)],
        **configs)


def _make_broker(capture):
    broker = MockBroker(broker_version=(2, 5))
    broker.set_metadata(topics=[_metadata_topic(_NUM_PARTITIONS)])
    broker.respond_always(ProduceRequest, capture)
    return broker


def test_sticky_rotation_sequence_batching_and_order(monkeypatch):
    """With a fixed random source, null-key records stick to one partition
    until the batch fills, then rotate to the next seeded pick. Verified
    on the sender's actual ProduceRequests: batch count, per-batch
    partition sequence, and record order within each partition.
    """
    seed = 20260925
    num_batches = 8
    capture = _ProduceCapture()
    broker = _make_broker(capture)
    producer = _make_producer(broker, monkeypatch, seed)
    try:
        for batch_no in range(num_batches):
            group = []
            for i in range(_PER_BATCH):
                n = batch_no * _PER_BATCH + i
                group.append(producer.send(_TOPIC, value=_value(n),
                                           timestamp_ms=_TS))
            # Serialize batches: wait for each batch to be acked before
            # sending the next, so the broker sees exactly one batch per
            # ProduceRequest in creation order.
            for future in group:
                future.get(timeout=10)
    finally:
        producer.close(timeout=10)

    # Exactly one batch per ProduceRequest, exactly num_batches batches.
    assert len(capture.batches) == num_batches, (
        'expected %d dense batches, got %d: %r'
        % (num_batches, len(capture.batches), capture.batches))

    # The partition sequence matches the seeded rotation replay.
    expected = _expected_batch_partitions(seed, _NUM_PARTITIONS, num_batches)
    actual = [partition for (_, partition, _) in capture.batches]
    assert actual == expected, (
        'batch partition sequence %r != seeded rotation %r' % (actual, expected))

    # Every batch is dense (full) and carries its records in send order.
    for batch_no, (topic, partition, values) in enumerate(capture.batches):
        assert topic == _TOPIC
        assert values == [_value(batch_no * _PER_BATCH + i)
                          for i in range(_PER_BATCH)], (
            'batch %d for partition %d has wrong records/order: %r'
            % (batch_no, partition, values))


def test_all_available_partitions_get_batches_over_time(monkeypatch):
    """Long run: rotation avoids only the immediately-previous partition,
    so over many batches every available partition must receive data.
    """
    seed = 7
    num_batches = 40
    capture = _ProduceCapture()
    broker = _make_broker(capture)
    producer = _make_producer(broker, monkeypatch, seed)
    try:
        futures = [
            producer.send(_TOPIC, value=_value(n), timestamp_ms=_TS)
            for n in range(num_batches * _PER_BATCH)
        ]
        for future in futures:
            future.get(timeout=10)
    finally:
        producer.close(timeout=10)

    assert len(capture.batches) == num_batches
    seen = {partition for (_, partition, _) in capture.batches}
    assert seen == set(range(_NUM_PARTITIONS)), (
        'partitions %r never received a batch'
        % (set(range(_NUM_PARTITIONS)) - seen,))
    # Dense packing throughout: every batch holds exactly _PER_BATCH records.
    for (_, partition, values) in capture.batches:
        assert len(values) == _PER_BATCH, (
            'partition %d got a non-dense batch: %r' % (partition, values))
    # Per-partition record order matches send order.
    sent_index = {_value(n): n for n in range(num_batches * _PER_BATCH)}
    per_partition = collections.defaultdict(list)
    for (_, partition, values) in capture.batches:
        per_partition[partition].extend(values)
    for partition, values in per_partition.items():
        indices = [sent_index[v] for v in values]
        assert indices == sorted(indices), (
            'partition %d received records out of order: %r' % (partition, values))


def test_retried_batch_is_never_repartitioned(monkeypatch):
    """A sealed batch that fails with a retriable error is re-enqueued
    as-is: the retry must go to the same partition with the same records,
    never re-partitioned. New records rotate to a fresh partition.
    """
    seed = 11
    capture = _ProduceCapture(
        fail_first_errno=Errors.NotLeaderForPartitionError.errno)
    broker = _make_broker(capture)
    producer = _make_producer(broker, monkeypatch, seed)
    try:
        # First batch: fills, drains, fails, gets retried. Wait for the
        # retry to succeed before sending more so the request sequence is
        # deterministic.
        first = [producer.send(_TOPIC, value=_value(n), timestamp_ms=_TS)
                 for n in range(_PER_BATCH)]
        for future in first:
            future.get(timeout=10)
        # Second batch: the sticky partition rotates exactly once.
        second = [producer.send(_TOPIC, value=_value(n), timestamp_ms=_TS)
                  for n in range(_PER_BATCH, 2 * _PER_BATCH)]
        for future in second:
            future.get(timeout=10)
    finally:
        producer.close(timeout=10)

    first_values = [_value(n) for n in range(_PER_BATCH)]
    second_values = [_value(n) for n in range(_PER_BATCH, 2 * _PER_BATCH)]

    # Request 1: first attempt (failed). Request 2: the retry - identical
    # partition and records. Request 3: the rotated second batch.
    assert len(capture.batches) == 3, (
        'expected failed attempt + retry + rotated batch, got %r'
        % (capture.batches,))
    (_, p_attempt, v_attempt) = capture.batches[0]
    (_, p_retry, v_retry) = capture.batches[1]
    (_, p_second, v_second) = capture.batches[2]

    assert v_attempt == first_values
    assert (p_retry, v_retry) == (p_attempt, first_values), (
        'retried batch moved from partition %d to %d or lost records'
        % (p_attempt, p_retry))
    assert v_second == second_values
    assert p_second != p_attempt, (
        'post-retry batch should have rotated off partition %d' % p_attempt)


def test_sticky_batching_with_compression(monkeypatch):
    """Compressed batches fill by uncompressed-size estimate (the
    compressed size is unknown until close): the abort-driven rotation
    must still produce dense batches following the seeded sequence.
    """
    seed = 3
    num_batches = 6
    capture = _ProduceCapture()
    broker = _make_broker(capture)
    producer = _make_producer(broker, monkeypatch, seed,
                              compression_type='gzip')
    try:
        for batch_no in range(num_batches):
            group = []
            for i in range(_PER_BATCH):
                n = batch_no * _PER_BATCH + i
                group.append(producer.send(_TOPIC, value=_value(n),
                                           timestamp_ms=_TS))
            for future in group:
                future.get(timeout=10)
    finally:
        producer.close(timeout=10)

    assert len(capture.batches) == num_batches
    expected = _expected_batch_partitions(seed, _NUM_PARTITIONS, num_batches)
    actual = [partition for (_, partition, _) in capture.batches]
    assert actual == expected
    for batch_no, (_, _, values) in enumerate(capture.batches):
        assert values == [_value(batch_no * _PER_BATCH + i)
                          for i in range(_PER_BATCH)]


def test_oversized_record_gets_own_batch(monkeypatch):
    """A record larger than batch_size always fits in a fresh batch (the
    first append is never rejected): it must land alone in its own batch,
    with surrounding small records batched normally around it.
    """
    seed = 5
    capture = _ProduceCapture()
    broker = _make_broker(capture)
    batch_size = _batch_size_for(_PER_BATCH)
    big_value = b'B' * (batch_size * 2)
    producer = _make_producer(broker, monkeypatch, seed)
    try:
        futures = []
        # Two small records (partial batch), then the oversized record,
        # then two more small records (partial batch, sent on flush).
        futures.append(producer.send(_TOPIC, value=_value(0), timestamp_ms=_TS))
        futures.append(producer.send(_TOPIC, value=_value(1), timestamp_ms=_TS))
        futures.append(producer.send(_TOPIC, value=big_value, timestamp_ms=_TS))
        futures.append(producer.send(_TOPIC, value=_value(2), timestamp_ms=_TS))
        futures.append(producer.send(_TOPIC, value=_value(3), timestamp_ms=_TS))
        producer.flush()
        for future in futures:
            future.get(timeout=10)
    finally:
        producer.close(timeout=10)

    # The oversized record is alone in its batch.
    big_batches = [values for (_, _, values) in capture.batches
                   if big_value in values]
    assert big_batches == [[big_value]], (
        'oversized record must be alone in its batch: %r' % (capture.batches,))

    # Every record arrived exactly once, and per-partition order is
    # preserved (send order: small, small, big, small, small).
    sent_order = [_value(0), _value(1), big_value, _value(2), _value(3)]
    position = {v: i for i, v in enumerate(sent_order)}
    per_partition = collections.defaultdict(list)
    for (_, partition, values) in capture.batches:
        per_partition[partition].extend(values)
    arrived = [v for values in per_partition.values() for v in values]
    assert sorted(arrived) == sorted(sent_order)
    for partition, values in per_partition.items():
        positions = [position[v] for v in values]
        assert positions == sorted(positions), (
            'partition %d received records out of order: %r' % (partition, values))
