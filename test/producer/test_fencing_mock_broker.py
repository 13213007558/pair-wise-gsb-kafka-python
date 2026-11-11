# pylint: skip-file
"""End-to-end fencing and epoch-recovery tests driven by MockBroker.

Two transactional producers sharing one transactional_id is the producer
fencing scenario: once the second producer wins the epoch, the old producer
must stop writing. These tests verify that fencing makes the *whole* client
consistent, not just the TransactionManager state enum:

  * the in-flight batch future and every still-queued batch future fail with
    ProducerFencedError;
  * no further ProduceRequest ever reaches the broker after the fence;
  * a stale success frame arriving after the fence cannot resurrect the
    failed batch (guarded at both the wire/correlation-id layer and the
    batch final-state layer);
  * the public API (begin/commit/abort/send/flush) refuses further work and
    close() drains cleanly.

A KIP-360 control case covers the recoverable counterpart: a sequence-related
produce error (UNKNOWN_PRODUCER_ID or OUT_OF_ORDER_SEQUENCE_NUMBER) on a
>= 2.5 broker bumps the producer epoch instead of going fatal, the queued
batch is re-sent under the new epoch and succeeds, and the producer returns
to READY.
"""

import time

import pytest

import kafka.errors as Errors
from kafka import KafkaProducer
from kafka.protocol.metadata import (
    FindCoordinatorRequest, FindCoordinatorResponse, MetadataResponse,
)
from kafka.protocol.producer import (
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse,
    InitProducerIdRequest, InitProducerIdResponse,
    ProduceRequest, ProduceResponse,
)
from kafka.structs import TopicPartition

from test.mock_broker import MockBroker

_TXN_ID = 'shared-txn-id'
_API_VERSION = (2, 5)
_PRODUCER_ID = 42


def _wait_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _contains_fencing_cause(error):
    """True if the error is, or wraps, a ProducerFencedError.

    The public transactional API raises a generic KafkaError once the
    TransactionManager is in FATAL_ERROR. The send gate keeps the fence as an
    exception argument (TransactionManager._fail_if_not_ready_for_send), while
    begin/commit/abort embed it in the message string
    (_maybe_fail_with_error); accept both shapes.
    """
    if isinstance(error, Errors.ProducerFencedError):
        return True
    for arg in getattr(error, 'args', ()):
        if isinstance(arg, BaseException) and _contains_fencing_cause(arg):
            return True
    return 'ProducerFencedError' in str(error)


class _FencingHarness:
    """Wires a MockBroker with the coordinator/partition handlers a
    transactional producer needs, plus a scriptable ProduceRequest path."""

    def __init__(self, topic, broker_version=_API_VERSION):
        self.topic = topic
        self.broker = MockBroker(broker_version=broker_version)
        self.broker.set_metadata(topics=[self._metadata_topic()])
        self.produce_requests = []
        self._install_coordinator_handlers()

    def _metadata_topic(self, version=8):
        Topic = MetadataResponse.MetadataResponseTopic
        Partition = Topic.MetadataResponsePartition
        return Topic(
            version=version, error_code=0, name=self.topic, is_internal=False,
            partitions=[Partition(
                version=version, error_code=0, partition_index=0,
                leader_id=0, leader_epoch=0, replica_nodes=[0],
                isr_nodes=[0], offline_replicas=[])])

    def _install_coordinator_handlers(self):
        broker = self.broker

        def find_coordinator(api_key, api_version, correlation_id, request_bytes):
            coordinator = FindCoordinatorResponse.Coordinator(
                key=_TXN_ID, node_id=broker.node_id, host=broker.host,
                port=broker.port, error_code=0, error_message='')
            return FindCoordinatorResponse(
                throttle_time_ms=0, error_code=0, error_message='',
                node_id=broker.node_id, host=broker.host, port=broker.port,
                coordinators=[coordinator])
        broker.respond_always(FindCoordinatorRequest, find_coordinator)

        def add_partitions(api_key, api_version, correlation_id, request_bytes):
            topic_result = AddPartitionsToTxnResponse.AddPartitionsToTxnTopicResult
            partition_result = topic_result.AddPartitionsToTxnPartitionResult
            return AddPartitionsToTxnResponse(
                throttle_time_ms=0,
                results_by_topic_v3_and_below=[topic_result(
                    name=self.topic,
                    results_by_partition=[partition_result(
                        partition_index=0, partition_error_code=0)])])
        broker.respond_always(AddPartitionsToTxnRequest, add_partitions)

    def script_init_producer_id(self, producer_id=_PRODUCER_ID, epoch=0):
        self.broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0, error_code=0,
            producer_id=producer_id, producer_epoch=epoch))

    def produce_response(self, error_code=0, base_offset=-1, log_start_offset=-1):
        topic = ProduceResponse.TopicProduceResponse
        partition = topic.PartitionProduceResponse
        return ProduceResponse(
            throttle_time_ms=0,
            responses=[topic(name=self.topic, partition_responses=[partition(
                index=0, error_code=error_code, base_offset=base_offset,
                log_append_time_ms=-1, log_start_offset=log_start_offset,
                record_errors=[], error_message=None,
                current_leader=None)])])

    def script_produce_fn(self, fn, count=16):
        for _ in range(count):
            self.broker.respond_fn(ProduceRequest, fn)

    def make_transactional_producer(self, **overrides):
        configs = dict(
            kafka_client=self.broker.client_factory(),
            bootstrap_servers=['%s:%d' % (self.broker.host, self.broker.port)],
            api_version=_API_VERSION,
            transactional_id=_TXN_ID,
            linger_ms=20,
            retry_backoff_ms=10,
            request_timeout_ms=5000,
        )
        configs.update(overrides)
        return KafkaProducer(**configs)

    def produce_connection(self):
        """Return the mock transport whose connection still has an in-flight
        ProduceRequest (the producer opens a second node-0 connection after
        bootstrap, so the transport set is not a singleton)."""
        for transport in list(self.broker._transports):
            protocol = getattr(transport, '_protocol', None)
            if protocol is not None and protocol.in_flight_requests:
                return transport
        raise AssertionError('no mock transport with an in-flight request')


class TestProducerFencing:
    """Fencing must poison the whole producer: state, batch queue, futures
    and the public API must all agree that no more writes are possible."""

    def test_fence_after_inflight_batch_fails_all_futures_and_stops_writes(self):
        harness = _FencingHarness('fence-topic')
        harness.script_init_producer_id()

        release = None

        def on_produce(api_key, api_version, correlation_id, request_bytes):
            harness.produce_requests.append(correlation_id)
            # Hold the first (and only expected) ProduceRequest until the
            # test has queued a second batch behind it.
            async def held():
                await release
                return harness.produce_response(
                    Errors.ProducerFencedError.errno)
            return held()

        harness.script_produce_fn(on_produce)

        producer = harness.make_transactional_producer()
        try:
            release = producer._net.create_future()
            producer.init_transactions()
            producer.begin_transaction()

            inflight_future = producer.send(harness.topic, value=b'in-flight', partition=0)
            assert _wait_until(lambda: len(harness.produce_requests) == 1)

            # A second record lands in a separate batch that never drains:
            # the partition is muted while the first batch is in flight
            # (transactional producers force guarantee_message_order).
            queued_future = producer.send(harness.topic, value=b'queued', partition=0)
            time.sleep(0.3)
            assert len(harness.produce_requests) == 1

            # Fencing arrives before the (stale) success would.
            release.success(None)

            for future in (inflight_future, queued_future):
                with pytest.raises(Errors.ProducerFencedError):
                    future.get(timeout=5)

            manager = producer._transaction_manager
            assert manager.has_fatal_error()
            assert manager._current_state.name == 'FATAL_ERROR'
            assert isinstance(manager.last_error, Errors.ProducerFencedError)

            # The batch queue is empty and nothing is left incomplete: the
            # queued batch was aborted, not silently dropped or retried.
            assert not producer._accumulator.has_incomplete
            queued_deque = producer._accumulator._batches[
                TopicPartition(harness.topic, 0)]
            assert len(queued_deque) == 0

            # No further produce traffic after the fence, even after a flush.
            assert len(harness.produce_requests) == 1
            producer.flush(timeout=5)
            assert len(harness.produce_requests) == 1
        finally:
            producer.close(timeout=5)

        # And still nothing new went on the wire during shutdown.
        assert len(harness.produce_requests) == 1

    def test_public_api_refuses_all_writes_after_fence(self):
        harness = _FencingHarness('fence-api-topic')
        harness.script_init_producer_id()
        release = None

        def on_produce(api_key, api_version, correlation_id, request_bytes):
            harness.produce_requests.append(correlation_id)

            async def held():
                await release
                return harness.produce_response(
                    Errors.ProducerFencedError.errno)
            return held()

        harness.script_produce_fn(on_produce)

        producer = harness.make_transactional_producer()
        try:
            release = producer._net.create_future()
            producer.init_transactions()
            producer.begin_transaction()
            future = producer.send(harness.topic, value=b'x', partition=0)
            assert _wait_until(lambda: len(harness.produce_requests) == 1)
            release.success(None)
            with pytest.raises(Errors.ProducerFencedError):
                future.get(timeout=5)

            # Every transactional entry point must surface the fatal state.
            # The public API raises a KafkaError wrapper; the underlying
            # ProducerFencedError is preserved as its cause/argument so
            # application error handling can still recognize the fence.
            for call in (
                producer.begin_transaction,
                producer.commit_transaction,
                producer.abort_transaction,
                lambda: producer.send(harness.topic, value=b'after-fence', partition=0),
            ):
                with pytest.raises(Errors.KafkaError) as exc_info:
                    call()
                assert _contains_fencing_cause(exc_info.value)

            # flush must not hang and must not trigger more writes.
            producer.flush(timeout=5)
            assert len(harness.produce_requests) == 1
        finally:
            producer.close(timeout=5)

    def test_stale_success_after_fence_cannot_resurrect_batch(self):
        """Two-layer guard against a delayed success reply:

        1. wire layer: the correlation id was already consumed by the fencing
           response, so a duplicate frame is rejected as CorrelationIdError
           and the connection is not treated as a successful produce;
        2. batch layer: an already-aborted ProducerBatch ignores a later
           success (final state is immutable once ABORTED).
        """
        harness = _FencingHarness('fence-stale-topic')
        harness.script_init_producer_id()
        release = None

        def on_produce(api_key, api_version, correlation_id, request_bytes):
            harness.produce_requests.append(correlation_id)

            async def held():
                await release
                return harness.produce_response(
                    Errors.ProducerFencedError.errno)
            return held()

        harness.script_produce_fn(on_produce)

        producer = harness.make_transactional_producer()
        try:
            release = producer._net.create_future()
            producer.init_transactions()
            producer.begin_transaction()
            future = producer.send(harness.topic, value=b'x', partition=0)
            assert _wait_until(lambda: len(harness.produce_requests) == 1)

            transport = harness.produce_connection()
            connection = transport._protocol
            correlation_id = harness.produce_requests[0]
            assert len(connection.in_flight_requests) == 1

            release.success(None)
            with pytest.raises(Errors.ProducerFencedError):
                future.get(timeout=5)
            assert producer._transaction_manager.has_fatal_error()
            assert len(connection.in_flight_requests) == 0

            # Simulate the broker's stale success frame for the same request.
            stale = harness.produce_response(error_code=0, base_offset=999)
            stale.API_VERSION = 8
            stale.with_header(correlation_id=correlation_id)
            with pytest.raises(Errors.CorrelationIdError):
                connection.data_received(
                    stale.encode(header=True, framed=True))

            # The fencing outcome is untouched by the rejected stale frame.
            assert future.failed()
            assert isinstance(future.exception, Errors.ProducerFencedError)
            assert producer._transaction_manager.has_fatal_error()
        finally:
            producer.close(timeout=5)

    def test_batch_final_state_ignores_late_success_after_abort(self):
        """Direct batch-level guard: once a batch is aborted with the fencing
        error, a later SUCCESS completion must be a no-op rather than flip the
        batch to SUCCEEDED (mirrors the Java client's final-state rules)."""
        from kafka.producer.producer_batch import ProducerBatch, FinalState
        from kafka.record.memory_records import MemoryRecordsBuilder

        tp = TopicPartition('fence-batch-topic', 0)
        records = MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=100000)
        batch = ProducerBatch(tp, records)
        batch.try_append(0, None, b'msg', [])
        batch.records.close()
        record_future = batch._record_futures[0]

        batch.abort(Errors.ProducerFencedError())
        assert batch.final_state == FinalState.ABORTED
        assert record_future.failed()
        assert isinstance(record_future.exception, Errors.ProducerFencedError)

        # A stale success returns False and leaves the batch aborted.
        assert batch.complete(base_offset=100, log_append_time=-1) is False
        assert batch.final_state == FinalState.ABORTED
        assert record_future.failed()
        assert isinstance(record_future.exception, Errors.ProducerFencedError)


class TestRecoverableSequenceErrorEpochBump:
    """Control group: a sequence-related produce error that KIP-360 says is
    recoverable must bump the epoch and keep the producer usable rather than
    being treated as a fatal fencing-style error."""

    @pytest.mark.parametrize('recoverable_error', [
        Errors.UnknownProducerIdError,
        Errors.OutOfOrderSequenceNumberError,
    ], ids=['unknown_producer_id', 'out_of_order_sequence'])
    def test_sequence_error_bumps_epoch_and_resumes_sending(self, recoverable_error):
        topic = 'bump-recovery-topic'
        broker = MockBroker(broker_version=_API_VERSION)

        metadata_topic = MetadataResponse.MetadataResponseTopic
        metadata_partition = metadata_topic.MetadataResponsePartition
        broker.set_metadata(topics=[metadata_topic(
            version=8, error_code=0, name=topic, is_internal=False,
            partitions=[metadata_partition(
                version=8, error_code=0, partition_index=0, leader_id=0,
                leader_epoch=0, replica_nodes=[0], isr_nodes=[0],
                offline_replicas=[])])])

        def init_producer_id(api_key, api_version, correlation_id, request_bytes):
            request = InitProducerIdRequest.decode(
                request_bytes, version=api_version, header=True)
            epoch = request.producer_epoch + 1 if request.producer_epoch >= 0 else 0
            return InitProducerIdResponse(
                throttle_time_ms=0, error_code=0,
                producer_id=_PRODUCER_ID, producer_epoch=epoch)
        broker.respond_always(InitProducerIdRequest, init_producer_id)

        produce_calls = []

        def produce_response(error_code=0, base_offset=-1):
            topic_response = ProduceResponse.TopicProduceResponse
            partition = topic_response.PartitionProduceResponse
            # log_start_offset=-1 keeps this out of the KAFKA-5793 retention
            # fast-path so the KIP-360 epoch-bump classifier is exercised.
            return ProduceResponse(
                throttle_time_ms=0,
                responses=[topic_response(name=topic, partition_responses=[
                    partition(index=0, error_code=error_code,
                              base_offset=base_offset, log_append_time_ms=-1,
                              log_start_offset=-1, record_errors=[],
                              error_message=None, current_leader=None)])])

        def on_produce(api_key, api_version, correlation_id, request_bytes):
            from kafka.record import MemoryRecords
            request = ProduceRequest.decode(
                request_bytes, version=api_version, header=True)
            record_batch = MemoryRecords(
                bytes(request.topic_data[0].partition_data[0].records)
            ).next_batch()
            produce_calls.append((
                record_batch.producer_id,
                record_batch.producer_epoch,
                record_batch.base_sequence,
            ))
            if len(produce_calls) == 1:
                return produce_response(recoverable_error.errno)
            return produce_response(error_code=0, base_offset=10)

        for _ in range(16):
            broker.respond_fn(ProduceRequest, on_produce)

        producer = KafkaProducer(
            kafka_client=broker.client_factory(),
            bootstrap_servers=['%s:%d' % (broker.host, broker.port)],
            api_version=_API_VERSION,
            enable_idempotence=True,
            batch_size=40,
            linger_ms=0,
            retry_backoff_ms=10,
            request_timeout_ms=5000,
        )
        try:
            inflight_future = producer.send(topic, value=b'aaaaaaaaaaaa', partition=0)
            assert _wait_until(lambda: len(produce_calls) == 1)

            queued_future = producer.send(topic, value=b'bbbbbbbbbbbb', partition=0)

            # The in-flight batch is the one the broker rejected: its future
            # fails with the broker error and is intentionally lost (KAFKA-5793
            # in-place rewrite is still a TODO in this client).
            with pytest.raises(recoverable_error):
                inflight_future.get(timeout=8)

            # The still-queued batch is re-sent under the bumped epoch...
            metadata = queued_future.get(timeout=8)
            assert metadata.offset == 10

            manager = producer._transaction_manager
            assert not manager.has_fatal_error()
            assert manager._current_state.name == 'READY'
            assert manager.producer_id_and_epoch.producer_id == _PRODUCER_ID
            assert manager.producer_id_and_epoch.epoch == 1

            # First wire call under epoch 0 fails; second call is the queued
            # batch re-stamped with epoch 1 and a fresh sequence 0.
            assert produce_calls == [
                (_PRODUCER_ID, 0, 0),
                (_PRODUCER_ID, 1, 0),
            ]
            assert not producer._accumulator.has_incomplete
        finally:
            producer.close(timeout=5)
