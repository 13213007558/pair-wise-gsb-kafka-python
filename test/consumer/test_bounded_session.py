# pylint: skip-file
"""Wire-level tests for BoundedReadSession.

Drives the session through a real client/manager stack wired to a
scriptable MockBroker: ListOffsets captures the per-partition boundary,
then scripted Fetch responses walk the position up to it. Covers offset
holes (compaction / deletion / transaction filtering), empty batches,
continuously appended new data, partition unavailability, read_committed
stable boundaries, and topic identity changes.
"""

import uuid

import pytest

import kafka.errors as Errors
from kafka.consumer.bounded import (
    BoundedReadSession, TopicIdentityChangedError,
)
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse,
    ListOffsetsRequest, ListOffsetsResponse,
)
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


TOPIC = 'foobar'

_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_MetaBroker = MetadataResponse.MetadataResponseBroker
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData
_ListTopic = ListOffsetsResponse.ListOffsetsTopicResponse
_ListPartition = _ListTopic.ListOffsetsPartitionResponse


# --------------------------------------------------------------------------- #
# Builders                                                                    #
# --------------------------------------------------------------------------- #


def _broker_metadata(broker, leader_epoch=3, partitions=(0, 1)):
    broker.set_metadata(
        brokers=[_MetaBroker(node_id=broker.node_id, host=broker.host,
                             port=broker.port, rack=None)],
        topics=[_MetaTopic(
            error_code=0, name=TOPIC, is_internal=False,
            partitions=[_MetaPartition(
                error_code=0, partition_index=p, leader_id=broker.node_id,
                leader_epoch=leader_epoch, replica_nodes=[broker.node_id],
                isr_nodes=[broker.node_id], offline_replicas=[])
                for p in partitions],
        )],
    )


def _build_record_batch(offsets, base_offset=None):
    """Build a v2 record batch holding one record per offset in `offsets`.

    Offsets need not be contiguous -- holes model compaction/deletion.
    """
    offsets = list(offsets)
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999,
        offset=offsets[0] if base_offset is None else base_offset)
    for off in offsets:
        builder.append(key=b'k', value=b'v%d' % off, timestamp=0, headers=[])
    builder.close()
    return builder.buffer()


def _list_offsets_handler(ends, captured=None):
    """respond_fn handler answering ListOffsets with the given end offsets."""
    def handler(api_key, api_version, correlation_id, request_bytes):
        if captured is not None:
            captured['version'] = api_version
        return ListOffsetsResponse(
            version=api_version,
            throttle_time_ms=0,
            topics=[_ListTopic(name=TOPIC, partitions=[
                _ListPartition(partition_index=p, error_code=0, timestamp=-1,
                               offset=end, leader_epoch=3,
                               old_style_offsets=[end])
                for p, end in ends.items()])])
    return handler


def _fetch_response(partitions_data, last_stable_offset=-1):
    """FetchResponse for TOPIC; partitions_data: {partition: (highwater, records)}."""
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[
            _FetchPartition(partition_index=p, error_code=0,
                            high_watermark=hw,
                            last_stable_offset=last_stable_offset,
                            log_start_offset=-1,
                            aborted_transactions=[],
                            preferred_read_replica=-1,
                            records=records)
            for p, (hw, records) in partitions_data.items()])])


def _empty_fetch(partitions, highwater):
    return _fetch_response({p: (highwater, b'') for p in partitions})


@pytest.fixture
def session_client(broker, net, manager, client):
    """Bootstrapped client with TOPIC metadata on the mock broker."""
    _broker_metadata(broker)
    manager.bootstrap(timeout_ms=5000)
    return client


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


class TestBoundedReadSession:

    def test_reads_exactly_to_captured_boundary(
            self, broker, session_client, metrics):
        """Two partitions, different starts/bounds: each is read over its
        own captured range and the session ends when every position
        reaches its boundary."""
        tp0 = TopicPartition(TOPIC, 0)
        tp1 = TopicPartition(TOPIC, 1)
        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 3, 1: 5}))
        broker.respond(FetchRequest, _fetch_response({
            0: (3, _build_record_batch([1, 2])),
            1: (5, _build_record_batch([0, 1, 2, 3, 4])),
        }))

        session = BoundedReadSession(
            session_client, {tp0: 1, tp1: 0}, metrics=metrics,
            retry_backoff_ms=50)
        boundaries = session.begin()
        assert boundaries[tp0].end_offset == 3
        assert boundaries[tp1].end_offset == 5

        result = session.collect(timeout_ms=1000)

        assert [r.offset for r in result.records[tp0]] == [1, 2]
        assert [r.offset for r in result.records[tp1]] == [0, 1, 2, 3, 4]
        assert session.done
        for tp, end in ((tp0, 3), (tp1, 5)):
            progress = result.progress[tp]
            assert progress.complete
            assert progress.position == end
            assert progress.end_offset == end

    def test_boundary_is_offset_range_not_record_count(
            self, broker, session_client, metrics):
        """Holes from compaction/deletion must not stall completion: the
        partition is done when its position reaches the boundary offset,
        however few records were actually delivered in between."""
        tp0 = TopicPartition(TOPIC, 0)
        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 11}))
        # Offsets 3..9 were compacted away; the log holds 0,1,2 and 10.
        broker.respond(FetchRequest, _fetch_response({
            0: (11, _build_record_batch([0, 1, 2]) + _build_record_batch([10])),
        }))

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics, retry_backoff_ms=50)
        result = session.collect(timeout_ms=1000)

        assert [r.offset for r in result.records[tp0]] == [0, 1, 2, 10]
        assert result.progress[tp0].position == 11
        assert session.done

    def test_empty_fetch_response_does_not_complete_session(
            self, broker, session_client, metrics):
        """An empty batch (broker had nothing new within fetch_max_wait)
        is not a completion signal; the session keeps reading until the
        position truly reaches the boundary."""
        tp0 = TopicPartition(TOPIC, 0)
        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 2}))
        broker.respond(FetchRequest, _empty_fetch([0], highwater=2))
        broker.respond(FetchRequest, _fetch_response({
            0: (2, _build_record_batch([0, 1])),
        }))

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics, retry_backoff_ms=50)
        first = session.poll(timeout_ms=1000)
        assert first == {}
        assert not session.done

        result = session.collect(timeout_ms=1000)
        assert [r.offset for r in result.records[tp0]] == [0, 1]
        assert session.done

    def test_new_appends_beyond_boundary_are_excluded(
            self, broker, session_client, metrics):
        """The log keeps growing after capture: the session stops at the
        originally captured boundary and drops records appended later,
        even when they arrive in the same fetch response."""
        tp0 = TopicPartition(TOPIC, 0)
        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 3}))

        fetches = [0]

        def append_handler(api_key, api_version, correlation_id, request_bytes):
            fetches[0] += 1
            # highwater runs far ahead of the captured boundary
            return _fetch_response({
                0: (100, _build_record_batch(list(range(10)))),
            })
        broker.respond_always(FetchRequest, append_handler)

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics, retry_backoff_ms=50)
        result = session.collect(timeout_ms=1000)

        assert [r.offset for r in result.records[tp0]] == [0, 1, 2]
        assert session.done
        assert result.progress[tp0].end_offset == 3
        # No further fetches once the boundary was crossed.
        assert fetches[0] == 1

    def test_partition_unavailable_is_retried(
            self, broker, session_client, metrics):
        """A retriable ListOffsets error (leader not yet elected) delays
        but does not fail boundary capture."""
        tp0 = TopicPartition(TOPIC, 0)
        list_offsets_calls = [0]

        def counting_handler(api_key, api_version, correlation_id, request_bytes):
            list_offsets_calls[0] += 1
            return _list_offsets_handler({0: 2})(
                api_key, api_version, correlation_id, request_bytes)

        broker.respond(ListOffsetsRequest, ListOffsetsResponse(
            version=4, throttle_time_ms=0,
            topics=[_ListTopic(name=TOPIC, partitions=[
                _ListPartition(partition_index=0,
                               error_code=Errors.NotLeaderForPartitionError.errno,
                               timestamp=-1, offset=-1, leader_epoch=-1,
                               old_style_offsets=[])])]))
        broker.respond_fn(ListOffsetsRequest, counting_handler)
        broker.respond(FetchRequest, _fetch_response({
            0: (2, _build_record_batch([0, 1])),
        }))

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics, retry_backoff_ms=50)
        result = session.collect(timeout_ms=2000)

        assert [r.offset for r in result.records[tp0]] == [0, 1]
        assert session.done
        # First call errored, second succeeded: capture really retried.
        assert list_offsets_calls[0] == 1

    def test_read_committed_uses_last_stable_offset_boundary(
            self, broker, session_client, metrics):
        """read_committed: the boundary is the LSO captured at start. A
        transaction committing *after* capture (LSO advances on later
        fetch responses) does not extend the session."""
        tp0 = TopicPartition(TOPIC, 0)
        captured = {}

        def list_offsets_handler(api_key, api_version, correlation_id, request_bytes):
            captured['version'] = api_version
            request = ListOffsetsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['isolation_level'] = request.isolation_level
            return _list_offsets_handler({0: 2})(
                api_key, api_version, correlation_id, request_bytes)
        broker.respond_fn(ListOffsetsRequest, list_offsets_handler)
        # Records 0,1 are stable; the fetch response already reports the
        # post-commit LSO (100) and carries the newly-committed records
        # 2..4 -- all beyond the captured boundary.
        broker.respond(FetchRequest, _fetch_response({
            0: (100, _build_record_batch(list(range(5)))),
        }, last_stable_offset=100))

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics,
            isolation_level='read_committed', retry_backoff_ms=50)
        result = session.collect(timeout_ms=1000)

        assert [r.offset for r in result.records[tp0]] == [0, 1]
        assert result.progress[tp0].end_offset == 2
        assert session.done
        # The boundary request went out as read_committed (broker answers
        # with the LSO, not the log end).
        from kafka.protocol.consumer import IsolationLevel
        assert captured['isolation_level'] == IsolationLevel.READ_COMMITTED

    def test_session_times_out_when_boundary_unreachable(
            self, broker, session_client, metrics):
        """If the remaining range can never be served (e.g. deleted after
        capture), the session raises KafkaTimeoutError with progress
        instead of waiting forever."""
        tp0 = TopicPartition(TOPIC, 0)
        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 10}))
        broker.respond_always(FetchRequest, _empty_fetch([0], highwater=10))

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics,
            timeout_ms=500, retry_backoff_ms=50)
        with pytest.raises(Errors.KafkaTimeoutError):
            session.collect(timeout_ms=100)
        progress = session.progress[tp0]
        assert not progress.complete
        assert progress.position == 0
        assert progress.end_offset == 10

    def test_unbounded_session_budget(
            self, broker, session_client, metrics):
        """timeout_ms=None disables the overall budget; per-call timeouts
        still apply."""
        tp0 = TopicPartition(TOPIC, 0)
        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 1}))
        broker.respond(FetchRequest, _fetch_response({
            0: (1, _build_record_batch([0])),
        }))

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics,
            timeout_ms=None, retry_backoff_ms=50)
        result = session.collect(timeout_ms=1000)
        assert [r.offset for r in result.records[tp0]] == [0]
        assert session.done

    def test_empty_range_completes_immediately(
            self, broker, session_client, metrics):
        """start >= captured end: nothing to read, no fetch issued."""
        tp0 = TopicPartition(TOPIC, 0)
        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 5}))

        session = BoundedReadSession(
            session_client, {tp0: 5}, metrics=metrics, retry_backoff_ms=50)
        result = session.collect(timeout_ms=1000)
        assert result.records == {}
        assert result.progress[tp0].complete
        assert session.done

    def test_constructor_validation(self, session_client):
        tp0 = TopicPartition(TOPIC, 0)
        with pytest.raises(ValueError):
            BoundedReadSession(session_client, {})
        with pytest.raises(ValueError):
            BoundedReadSession(session_client, {tp0: -1})
        with pytest.raises(TypeError):
            BoundedReadSession(session_client, {'not-a-tp': 0})
        with pytest.raises(Errors.KafkaConfigurationError):
            BoundedReadSession(session_client, {tp0: 0}, bogus_config=1)


class TestTopicIdentity:

    def _metadata_with_topic_id(self, broker, topic_id):
        """Install a persistent Metadata handler carrying topic_id (v10+)."""
        def handler(api_key, api_version, correlation_id, request_bytes):
            version = min(api_version, 12)
            return MetadataResponse(
                version=version,
                throttle_time_ms=0,
                brokers=[_MetaBroker(node_id=broker.node_id, host=broker.host,
                                     port=broker.port, rack=None)],
                cluster_id='mock-cluster',
                controller_id=broker.node_id,
                topics=[_MetaTopic(
                    error_code=0, name=TOPIC, topic_id=topic_id,
                    is_internal=False,
                    partitions=[_MetaPartition(
                        error_code=0, partition_index=0,
                        leader_id=broker.node_id, leader_epoch=3,
                        replica_nodes=[broker.node_id],
                        isr_nodes=[broker.node_id], offline_replicas=[])],
                    authorized_operations=set())],
                authorized_operations=set())
        broker.respond_always(MetadataRequest, handler)

    def test_topic_recreation_aborts_session(
            self, broker, net, manager, session_client, metrics):
        """Topic deleted and recreated under the same name mid-session:
        the topic_id change terminates the session instead of reading
        the new log."""
        tp0 = TopicPartition(TOPIC, 0)
        topic_id = uuid.uuid4()
        self._metadata_with_topic_id(broker, topic_id)
        net.wait_for(manager.cluster.request_update(), 2000)
        assert manager.cluster.topic_id(TOPIC) == topic_id

        broker.respond_fn(ListOffsetsRequest, _list_offsets_handler({0: 10}))
        broker.respond_always(FetchRequest, _empty_fetch([0], highwater=10))

        session = BoundedReadSession(
            session_client, {tp0: 0}, metrics=metrics, retry_backoff_ms=50)
        session.begin()

        # Recreate the topic: same name, new identity.
        self._metadata_with_topic_id(broker, uuid.uuid4())
        net.wait_for(manager.cluster.request_update(), 2000)

        with pytest.raises(TopicIdentityChangedError):
            session.poll(timeout_ms=1000)
        assert not session.done
