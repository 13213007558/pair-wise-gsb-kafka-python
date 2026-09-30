# pylint: skip-file
import uuid

import pytest

import kafka.errors as Errors
from kafka.consumer.bounded import (
    BoundedFetchSession, TopicRecreatedError,
)
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
)
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


TOPIC = 'bounded-topic'
TOPIC_ID = uuid.UUID('01234567-89ab-cdef-0123-456789abcdef')
REPLACED_TOPIC_ID = uuid.UUID('fedcba98-7654-3210-fedc-ba9876543210')


_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_MetaBroker = MetadataResponse.MetadataResponseBroker
_ListTopic = ListOffsetsResponse.ListOffsetsTopicResponse
_ListPartition = _ListTopic.ListOffsetsPartitionResponse
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData


def _record_batch(first_offset, values, transactional=False):
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999,
        offset=first_offset, transactional=transactional,
        producer_id=7 if transactional else -1,
        producer_epoch=1 if transactional else -1,
        base_sequence=0 if transactional else -1)
    for value in values:
        builder.append(key=None, value=value, timestamp=0, headers=[])
    builder.close()
    return builder.buffer()


def _record_batch_at_offsets(offsets, values, transactional=False):
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999,
        offset=offsets[0], transactional=transactional,
        producer_id=7 if transactional else -1,
        producer_epoch=1 if transactional else -1,
        base_sequence=0 if transactional else -1)
    previous = offsets[0] - 1
    for offset, value in zip(offsets, values):
        builder.skip(offset - previous - 1)
        builder.append(key=None, value=value, timestamp=0, headers=[])
        previous = offset
    builder.close()
    return builder.buffer()


def _metadata(broker, topic_id=TOPIC_ID, leader_id=0):
    return MetadataResponse(
        version=10,
        throttle_time_ms=0,
        brokers=[_MetaBroker(
            node_id=broker.node_id, host=broker.host, port=broker.port,
            rack=None, version=10)],
        cluster_id='mock-cluster',
        controller_id=broker.node_id,
        topics=[_MetaTopic(
            version=10,
            error_code=0,
            name=TOPIC,
            topic_id=topic_id,
            is_internal=False,
            partitions=[_MetaPartition(
                version=10,
                error_code=0,
                partition_index=0,
                leader_id=leader_id,
                leader_epoch=4,
                replica_nodes=[leader_id],
                isr_nodes=[leader_id],
                offline_replicas=[])])])


def _list_offsets(boundary):
    return ListOffsetsResponse(
        version=7,
        throttle_time_ms=0,
        topics=[_ListTopic(
            name=TOPIC,
            partitions=[_ListPartition(
                partition_index=0,
                error_code=0,
                timestamp=-1,
                offset=boundary,
                leader_epoch=4)])])


def _fetch(records=b'', error_code=0, high_watermark=100, last_stable_offset=100):
    return FetchResponse(
        version=12,
        throttle_time_ms=0,
        error_code=0,
        session_id=0,
        responses=[_FetchTopic(
            topic=TOPIC,
            partitions=[_FetchPartition(
                partition_index=0,
                error_code=error_code,
                high_watermark=high_watermark,
                last_stable_offset=last_stable_offset,
                log_start_offset=-1,
                aborted_transactions=[],
                preferred_read_replica=-1,
                records=records)])])


def _session(client, tp, start, boundary, **configs):
    config = {
        'default_api_timeout_ms': 5000,
        'retry_backoff_ms': 10,
        'max_poll_records': 500,
        'isolation_level': 'read_uncommitted',
        'check_crcs': True,
        'client_rack': '',
        'metadata_max_age_ms': 300000,
    }
    config.update(configs)
    return BoundedFetchSession(client, [tp], {tp: start}, **config)


def _decode_request(handler_payload, request_class):
    _, api_version, _, request_bytes = handler_payload
    return request_class.decode(
        request_bytes, version=api_version, header=True)


def test_bounded_session_ends_at_original_boundary_with_gaps_and_appends(
        broker, client, manager):
    tp = TopicPartition(TOPIC, 0)
    broker.respond(MetadataRequest, _metadata(broker))
    broker.respond(ListOffsetsRequest, _list_offsets(10))
    # First fetch returns an empty response despite continuous later appends.
    broker.respond(FetchRequest, _fetch(
        b'', high_watermark=12, last_stable_offset=12))
    # Compaction deleted offsets 0-2; the same response also carries appends
    # made after boundary capture. Only offset 9 is visible, and progress ends
    # at offset 10 rather than following the live log to 12.
    broker.respond(FetchRequest, _fetch(
        _record_batch_at_offsets([3, 4, 9, 10, 11],
                                 [b'3', b'4', b'9', b'10', b'11']),
        high_watermark=12, last_stable_offset=12))

    session = _session(client, tp, 0, 10)
    assert session.boundaries() == {tp: 10}
    assert session.poll() == {}

    records = session.poll()
    assert [record.offset for record in records[tp]] == [3, 4, 9]
    assert session.finished()
    assert session.progress() == {tp: 10}
    assert session.result() == {tp: (10, 10)}
    session.close()


def test_bounded_session_retries_partition_unavailable(
        broker, client, manager):
    tp = TopicPartition(TOPIC, 0)
    broker.respond(MetadataRequest, _metadata(broker))
    broker.respond(ListOffsetsRequest, _list_offsets(2))
    broker.respond(FetchRequest, _fetch(
        error_code=Errors.NotLeaderForPartitionError.errno))
    broker.respond(MetadataRequest, _metadata(broker))
    broker.respond(FetchRequest, _fetch(
        _record_batch(0, [b'0', b'1']),
        high_watermark=2, last_stable_offset=2))

    with _session(client, tp, 0, 2) as session:
        assert session.poll() == {}
        records = session.poll()
        assert [record.offset for record in records[tp]] == [0, 1]
        assert session.result() == {tp: (2, 2)}


def test_bounded_session_read_committed_uses_lso_and_caps_post_capture_commit(
        broker, client, manager):
    tp = TopicPartition(TOPIC, 0)
    list_requests = []
    fetch_requests = []

    def list_handler(*args):
        list_requests.append(_decode_request(args, ListOffsetsRequest))
        return _list_offsets(3)

    def fetch_handler(*args):
        fetch_requests.append(_decode_request(args, FetchRequest))
        records_data = (
            _record_batch(2, [b'2']) +
            _record_batch(3, [b'3', b'4'], transactional=True))
        return _fetch(
            records_data,
            high_watermark=6, last_stable_offset=6)

    broker.respond(MetadataRequest, _metadata(broker))
    broker.respond_fn(ListOffsetsRequest, list_handler)
    broker.respond_fn(FetchRequest, fetch_handler)

    with _session(client, tp, 2, 3, isolation_level='read_committed') as session:
        records = session.poll()
        assert [record.offset for record in records[tp]] == [2]
        assert session.result() == {tp: (3, 3)}

    assert list_requests[0].isolation_level == 1
    assert fetch_requests[0].isolation_level == 1


def test_bounded_session_stops_when_topic_is_recreated_with_same_name(
        broker, client, manager):
    tp = TopicPartition(TOPIC, 0)
    broker.respond(MetadataRequest, _metadata(broker, TOPIC_ID))
    broker.respond(ListOffsetsRequest, _list_offsets(5))
    broker.respond(FetchRequest, _fetch(
        error_code=Errors.UnknownTopicOrPartitionError.errno))
    broker.respond(MetadataRequest, _metadata(broker, REPLACED_TOPIC_ID))

    session = _session(client, tp, 0, 5)
    assert session.poll() == {}
    with pytest.raises(TopicRecreatedError):
        session.poll()
    session.close()
