"""Shared scaffolding for the ACL-revocation demos.

A real TCP broker is not required: kafka-python ships a scriptable
``MockBroker`` (test/mock_broker.py) that speaks the actual wire protocol over
an in-memory transport. We script it to emulate

  1. a healthy broker returning one batch of records, then
  2. a broker on which the READ ACL has been removed, so every later
     FetchResponse carries TOPIC_AUTHORIZATION_FAILED (error code 29).

Everything below uses only public-ish kafka-python classes plus the repo's
test mock; the point of the demos is timing, not a particular broker version.
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from kafka.protocol.consumer import (
    FetchResponse, ListOffsetsResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition
import kafka.errors as Errors

from test.mock_broker import MockBroker

TOPIC = 'security-events'
PARTITION = 0
TP = TopicPartition(TOPIC, PARTITION)
AUTH_DENIED = Errors.TopicAuthorizationFailedError.errno  # 29

_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_MetaBroker = MetadataResponse.MetadataResponseBroker
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData
_ListTopic = ListOffsetsResponse.ListOffsetsTopicResponse
_ListPartition = _ListTopic.ListOffsetsPartitionResponse


def build_records(messages, offset=0):
    """Encode plain (key=None) records exactly as a broker batch would be."""
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999, offset=offset)
    for i, value in enumerate(messages):
        builder.append(key=None, value=value, timestamp=1_700_000_000_000 + i,
                       headers=[])
    builder.close()
    return builder.buffer()


def metadata_response(broker):
    return [
        _MetaBroker(node_id=broker.node_id, host=broker.host,
                    port=broker.port, rack=None)], [
        _MetaTopic(error_code=0, name=TOPIC, is_internal=False, partitions=[
            _MetaPartition(error_code=0, partition_index=PARTITION,
                           leader_id=broker.node_id, leader_epoch=0,
                           replica_nodes=[broker.node_id],
                           isr_nodes=[broker.node_id],
                           offline_replicas=[])])]


def fetch_ok(messages, offset=0):
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=0, high_watermark=1000,
            last_stable_offset=-1, log_start_offset=-1,
            aborted_transactions=[], preferred_read_replica=-1,
            records=build_records(messages, offset=offset))])])


def fetch_denied():
    """FetchResponse with TOPIC_AUTHORIZATION_FAILED on our partition."""
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=AUTH_DENIED,
            high_watermark=-1, last_stable_offset=-1, log_start_offset=-1,
            aborted_transactions=[], preferred_read_replica=-1,
            records=b'')])])


def list_offsets_response(offset=0):
    # ListOffsets v5: (partition, error_code, timestamp, offset[, leader_epoch])
    return ListOffsetsResponse[5](
        0, [_ListTopic(TOPIC, [
            _ListPartition(PARTITION, 0, -1, offset, 0, version=5)],
            version=5)])


def ofle_response():
    topic = OffsetForLeaderEpochResponse.OffsetForLeaderTopicResult
    part = topic.EpochEndOffset
    return OffsetForLeaderEpochResponse(
        throttle_time_ms=0,
        topics=[topic(topic=TOPIC, partitions=[part(
            error_code=0, partition=PARTITION, leader_epoch=-1,
            end_offset=-1)])])


def make_broker():
    broker = MockBroker(broker_version=(3, 5))
    brokers, topics = metadata_response(broker)
    broker.set_metadata(brokers=brokers, topics=topics)
    return broker
