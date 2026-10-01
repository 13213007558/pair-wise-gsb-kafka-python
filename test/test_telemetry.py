import time
import uuid

import pytest

import kafka.errors as Errors
from kafka import codec
from kafka.metrics import AnonMeasurable, Metrics
from kafka.metrics.stats import Total
from kafka.protocol.telemetry import (
    GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsResponse,
    PushTelemetryRequest,
    PushTelemetryResponse,
)
from kafka.telemetry import (
    ClientTelemetryManager,
    ClientTelemetryReporter,
    MetricsEmitter,
)
from kafka.telemetry import otel
from kafka.telemetry.emitter import (
    COMPRESSION_GZIP,
    COMPRESSION_NONE,
    DEFAULT_MAX_PAYLOAD_BYTES,
    _matches_prefixes,
    otel_metric_name,
)


class FakeClock:
    def __init__(self, start_ms=1_000_000):
        self.now_ms = start_ms

    def __call__(self):
        return self.now_ms

    def advance(self, ms):
        self.now_ms += ms


class FakeTelemetryPeer:
    """A fake broker-side telemetry endpoint speaking real protocol objects."""

    def __init__(self):
        self.client_instance_id = uuid.uuid4()
        self.push_interval_ms = 30000
        self.delta_temporality = False
        self.accepted_compression_types = [COMPRESSION_NONE]
        self.requested_metrics = ['*']
        self.get_error_code = 0
        self.push_error_code = 0
        self.fail_next = 0
        self.subscription_id = 0
        self.requests = []
        self.decoded_pushes = []

    def least_loaded_node(self, bootstrap_fallback=False):
        return 0

    def send_and_receive(self, node_id, request, timeout_ms=None):
        if self.fail_next:
            self.fail_next -= 1
            raise Errors.KafkaConnectionError('fake disconnect')
        self.requests.append(request)
        if isinstance(request, GetTelemetrySubscriptionsRequest):
            self.subscription_id += 1
            return GetTelemetrySubscriptionsResponse[0](
                throttle_time_ms=0,
                error_code=self.get_error_code,
                client_instance_id=self.client_instance_id,
                subscription_id=self.subscription_id,
                accepted_compression_types=list(self.accepted_compression_types),
                push_interval_ms=self.push_interval_ms,
                delta_temporality=self.delta_temporality,
                requested_metrics=list(self.requested_metrics))
        elif isinstance(request, PushTelemetryRequest):
            payload = request.metrics
            if request.compression_type == COMPRESSION_GZIP:
                payload = codec.gzip_decode(payload)
            self.decoded_pushes.append((request, otel.decode_metrics_data(payload)))
            return PushTelemetryResponse[0](
                throttle_time_ms=0, error_code=self.push_error_code)
        raise ValueError('Unexpected request: %r' % (request,))


def decoded_metrics(decoded):
    result = {}
    for resource_metrics in decoded['resource_metrics']:
        for scope_metrics in resource_metrics['scope_metrics']:
            for metric in scope_metrics['metrics']:
                result[metric['name']] = metric
    return result


def make_metrics():
    metrics = Metrics()
    gauge_state = {'value': 2.5}
    metrics.add_metric(
        metrics.metric_name('latency-avg', 'producer-metrics', 'avg latency'),
        AnonMeasurable(lambda config, now: gauge_state['value']))
    total = Total()
    total.record(None, 5.0, 0)
    metrics.add_metric(
        metrics.metric_name('request-total', 'producer-metrics', 'total requests'),
        total)
    return metrics, gauge_state, total


def make_manager(clock, metrics=None, max_payload_bytes=DEFAULT_MAX_PAYLOAD_BYTES,
                 retry_backoff_ms=100):
    if metrics is None:
        metrics, _, _ = make_metrics()
    emitter = MetricsEmitter(metrics, client_id='test-client',
                             max_payload_bytes=max_payload_bytes, time_ms=clock)
    manager = ClientTelemetryManager(emitter, retry_backoff_ms=retry_backoff_ms,
                                     time_ms=clock)
    return manager, emitter, metrics


def subscribe(manager, peer):
    request = manager.next_request()
    assert isinstance(request, GetTelemetrySubscriptionsRequest)
    manager.handle_response(peer.send_and_receive(0, request))
    assert manager.state == ClientTelemetryManager.PUSH_NEEDED


def push_once(manager, peer, clock):
    clock.advance(peer.push_interval_ms)
    request = manager.next_request()
    assert isinstance(request, PushTelemetryRequest)
    manager.handle_response(peer.send_and_receive(0, request))
    return request, peer.decoded_pushes[-1][1]


def test_otel_encode_decode_roundtrip():
    payload = otel.encode_metrics_data(
        [{'name': 'a.gauge', 'gauge': {'data_points': [
            {'attributes': {'k': 'v'}, 'time_unix_nano': 7, 'as_double': 1.5}]}},
         {'name': 'a.sum', 'sum': {
             'data_points': [{'start_time_unix_nano': 3, 'time_unix_nano': 7,
                              'as_int': 42}],
             'aggregation_temporality': otel.AGGREGATION_TEMPORALITY_DELTA,
             'is_monotonic': True}}],
        resource_attributes={'client.id': 'c1'},
        scope_name='kafka-python', scope_version='1.0')
    decoded = otel.decode_metrics_data(payload)
    assert len(decoded['resource_metrics']) == 1
    rm = decoded['resource_metrics'][0]
    assert rm['resource_attributes'] == {'client.id': 'c1'}
    assert rm['scope_metrics'][0]['scope'] == {'name': 'kafka-python',
                                               'version': '1.0'}
    metrics = decoded_metrics(decoded)
    assert metrics['a.gauge']['gauge']['data_points'][0]['as_double'] == 1.5
    assert metrics['a.gauge']['gauge']['data_points'][0]['attributes'] == {'k': 'v'}
    point = metrics['a.sum']['sum']['data_points'][0]
    assert point['as_int'] == 42
    assert point['start_time_unix_nano'] == 3
    assert metrics['a.sum']['sum']['is_monotonic'] is True
    assert (metrics['a.sum']['sum']['aggregation_temporality']
            == otel.AGGREGATION_TEMPORALITY_DELTA)


def test_protocol_messages_roundtrip():
    instance_id = uuid.uuid4()
    request = GetTelemetrySubscriptionsRequest[0](client_instance_id=None)
    decoded = GetTelemetrySubscriptionsRequest[0].decode(request.encode())
    assert decoded.client_instance_id is None

    response = GetTelemetrySubscriptionsResponse[0](
        throttle_time_ms=0, error_code=0, client_instance_id=instance_id,
        subscription_id=3, accepted_compression_types=[1, 0],
        push_interval_ms=5000, delta_temporality=True,
        requested_metrics=['producer.'])
    decoded = GetTelemetrySubscriptionsResponse[0].decode(response.encode())
    assert decoded.client_instance_id == instance_id
    assert decoded.subscription_id == 3
    assert decoded.accepted_compression_types == [1, 0]
    assert decoded.delta_temporality is True

    push = PushTelemetryRequest[0](client_instance_id=instance_id,
                                   subscription_id=3, terminating=True,
                                   compression_type=1, metrics=b'payload')
    decoded = PushTelemetryRequest[0].decode(push.encode())
    assert decoded.terminating is True
    assert decoded.metrics == b'payload'

    push_response = PushTelemetryResponse[0](throttle_time_ms=0, error_code=117)
    decoded = PushTelemetryResponse[0].decode(push_response.encode())
    assert decoded.error_code == 117


def test_otel_metric_name_and_prefix_matching():
    metrics, _, _ = make_metrics()
    metric_name = next(n for n in metrics.metrics if n.name == 'latency-avg')
    assert otel_metric_name(metric_name) == 'producer.metrics.latency.avg'
    assert _matches_prefixes(['*'], 'anything.at.all')
    assert _matches_prefixes(['producer.metrics'], 'producer.metrics.latency.avg')
    assert _matches_prefixes(['producer.metrics.'], 'producer.metrics.latency.avg')
    assert _matches_prefixes(['producer.metrics.latency.avg'],
                             'producer.metrics.latency.avg')
    assert not _matches_prefixes(['producer.metrics.lat'],
                                 'producer.metrics.latency.avg')
    assert not _matches_prefixes(['consumer.metrics'],
                                 'producer.metrics.latency.avg')


def test_subscription_then_cumulative_push():
    clock = FakeClock()
    manager, _, metrics = make_manager(clock)
    peer = FakeTelemetryPeer()

    request = manager.next_request()
    assert isinstance(request, GetTelemetrySubscriptionsRequest)
    assert request.client_instance_id is None
    manager.handle_response(peer.send_and_receive(0, request))

    assert manager.state == ClientTelemetryManager.PUSH_NEEDED
    assert manager.subscription.client_instance_id == peer.client_instance_id
    assert manager.time_to_next_update_ms() == peer.push_interval_ms
    # Not yet time for the first push
    assert manager.next_request() is None

    request, decoded = push_once(manager, peer, clock)
    assert request.terminating is False
    assert request.subscription_id == 1
    assert request.client_instance_id == peer.client_instance_id
    assert request.compression_type == COMPRESSION_NONE

    rm = decoded['resource_metrics'][0]
    assert rm['resource_attributes'] == {'client.id': 'test-client'}
    pushed = decoded_metrics(decoded)
    gauge = pushed['producer.metrics.latency.avg']['gauge']['data_points'][0]
    assert gauge['as_double'] == 2.5
    counter = pushed['producer.metrics.request.total']['sum']
    assert counter['is_monotonic'] is True
    assert (counter['aggregation_temporality']
            == otel.AGGREGATION_TEMPORALITY_CUMULATIVE)
    point = counter['data_points'][0]
    assert point['as_int'] == 5
    assert point['time_unix_nano'] == clock.now_ms * 10 ** 6
    assert point['start_time_unix_nano'] == 1_000_000 * 10 ** 6


def test_delta_temporality():
    clock = FakeClock()
    manager, _, metrics = make_manager(clock)
    peer = FakeTelemetryPeer()
    peer.delta_temporality = True
    subscribe(manager, peer)

    _, decoded = push_once(manager, peer, clock)
    counter = decoded_metrics(decoded)['producer.metrics.request.total']['sum']
    assert counter['aggregation_temporality'] == otel.AGGREGATION_TEMPORALITY_DELTA
    first_collect_ms = clock.now_ms
    point = counter['data_points'][0]
    # First delta after a (re)subscription reports the full current value
    assert point['as_int'] == 5
    assert point['start_time_unix_nano'] == 1_000_000 * 10 ** 6

    total = next(m for n, m in metrics.metrics.items()
                 if n.name == 'request-total').measurable
    total.record(None, 4.0, 0)
    _, decoded = push_once(manager, peer, clock)
    counter = decoded_metrics(decoded)['producer.metrics.request.total']['sum']
    point = counter['data_points'][0]
    assert point['as_int'] == 4
    assert point['start_time_unix_nano'] == first_collect_ms * 10 ** 6


def test_counter_reset_on_unknown_subscription():
    clock = FakeClock()
    manager, _, _ = make_manager(clock)
    peer = FakeTelemetryPeer()
    peer.delta_temporality = True
    subscribe(manager, peer)
    _, decoded = push_once(manager, peer, clock)
    assert decoded_metrics(decoded)['producer.metrics.request.total'] \
        ['sum']['data_points'][0]['as_int'] == 5

    # Broker forgets the subscription (e.g. restart): the client must fetch a
    # new subscription and reset its delta baselines.
    peer.push_error_code = Errors.UnknownSubscriptionIdError.errno
    clock.advance(peer.push_interval_ms)
    request = manager.next_request()
    assert isinstance(request, PushTelemetryRequest)
    manager.handle_response(peer.send_and_receive(0, request))
    assert manager.state == ClientTelemetryManager.SUBSCRIPTION_NEEDED

    peer.push_error_code = 0
    request = manager.next_request()
    assert isinstance(request, GetTelemetrySubscriptionsRequest)
    assert request.client_instance_id == peer.client_instance_id
    manager.handle_response(peer.send_and_receive(0, request))
    assert manager.subscription.subscription_id == 2

    # Baselines were reset: the full cumulative value is reported again.
    _, decoded = push_once(manager, peer, clock)
    assert decoded_metrics(decoded)['producer.metrics.request.total'] \
        ['sum']['data_points'][0]['as_int'] == 5


def test_counter_reset_on_new_client_instance_id():
    clock = FakeClock()
    manager, emitter, _ = make_manager(clock)
    peer = FakeTelemetryPeer()
    peer.delta_temporality = True
    subscribe(manager, peer)
    push_once(manager, peer, clock)

    # Broker session change: same subscription id, new client instance id.
    peer.client_instance_id = uuid.uuid4()
    peer.push_error_code = Errors.UnknownSubscriptionIdError.errno
    clock.advance(peer.push_interval_ms)
    manager.handle_response(peer.send_and_receive(0, manager.next_request()))
    peer.push_error_code = 0
    peer.subscription_id -= 1  # keep the same subscription id
    manager.handle_response(peer.send_and_receive(0, manager.next_request()))
    assert manager.subscription.client_instance_id == peer.client_instance_id

    _, decoded = push_once(manager, peer, clock)
    assert decoded_metrics(decoded)['producer.metrics.request.total'] \
        ['sum']['data_points'][0]['as_int'] == 5


def test_oversized_payload_dropped_not_split():
    clock = FakeClock()
    manager, emitter, metrics = make_manager(clock, max_payload_bytes=16)
    peer = FakeTelemetryPeer()
    peer.delta_temporality = True
    subscribe(manager, peer)

    # Payload exceeds the limit: dropped whole, no request is produced and
    # nothing is retried/split (which would double-count deltas).
    clock.advance(peer.push_interval_ms)
    assert manager.next_request() is None
    assert manager.time_to_next_update_ms() == peer.push_interval_ms
    assert not peer.decoded_pushes

    # Broker-side rejection is handled the same way: drop, don't retry.
    emitter._max_payload_bytes = DEFAULT_MAX_PAYLOAD_BYTES
    peer.push_error_code = Errors.TelemetryTooLargeError.errno
    clock.advance(peer.push_interval_ms)
    request = manager.next_request()
    assert isinstance(request, PushTelemetryRequest)
    manager.handle_response(peer.send_and_receive(0, request))
    assert manager.state == ClientTelemetryManager.PUSH_NEEDED
    assert manager.time_to_next_update_ms() == peer.push_interval_ms

    # The dropped interval is not re-reported: only the new increment shows up.
    peer.push_error_code = 0
    total = next(m for n, m in metrics.metrics.items()
                 if n.name == 'request-total').measurable
    total.record(None, 4.0, 0)
    _, decoded = push_once(manager, peer, clock)
    assert decoded_metrics(decoded)['producer.metrics.request.total'] \
        ['sum']['data_points'][0]['as_int'] == 4


def test_terminating_push():
    clock = FakeClock()
    manager, _, _ = make_manager(clock)
    peer = FakeTelemetryPeer()
    subscribe(manager, peer)

    manager.initiate_close()
    assert manager.state == ClientTelemetryManager.TERMINATING_PUSH_NEEDED
    assert manager.time_to_next_update_ms() == 0
    request = manager.next_request()
    assert isinstance(request, PushTelemetryRequest)
    assert request.terminating is True
    manager.handle_response(peer.send_and_receive(0, request))
    assert manager.terminated
    assert manager.time_to_next_update_ms() is None
    assert manager.next_request() is None


def test_close_before_subscription_terminates_immediately():
    clock = FakeClock()
    manager, _, _ = make_manager(clock)
    manager.initiate_close()
    assert manager.terminated
    assert manager.next_request() is None


def test_compression_negotiation():
    clock = FakeClock()
    manager, _, _ = make_manager(clock)
    peer = FakeTelemetryPeer()
    peer.accepted_compression_types = [COMPRESSION_GZIP]
    subscribe(manager, peer)
    request, decoded = push_once(manager, peer, clock)
    assert request.compression_type == COMPRESSION_GZIP
    # The fake peer gunzipped before decoding; verify the raw payload too.
    raw = codec.gzip_decode(request.metrics)
    assert decoded_metrics(otel.decode_metrics_data(raw))

    # Broker accepts nothing the client supports -> no compression.
    clock2 = FakeClock()
    manager2, _, _ = make_manager(clock2)
    peer2 = FakeTelemetryPeer()
    peer2.accepted_compression_types = []
    subscribe(manager2, peer2)
    request, _ = push_once(manager2, peer2, clock2)
    assert request.compression_type == COMPRESSION_NONE


def test_requested_metrics_filter():
    clock = FakeClock()
    manager, _, _ = make_manager(clock)
    peer = FakeTelemetryPeer()
    peer.requested_metrics = ['producer.metrics.latency']
    subscribe(manager, peer)
    _, decoded = push_once(manager, peer, clock)
    pushed = decoded_metrics(decoded)
    assert 'producer.metrics.latency.avg' in pushed
    assert 'producer.metrics.request.total' not in pushed


def test_request_failure_backoff():
    clock = FakeClock()
    manager, _, _ = make_manager(clock, retry_backoff_ms=100)
    manager.handle_request_failure(Errors.KafkaConnectionError('boom'))
    assert manager.time_to_next_update_ms() == 100
    assert manager.next_request() is None
    clock.advance(100)
    assert isinstance(manager.next_request(), GetTelemetrySubscriptionsRequest)


def test_subscription_error_retried():
    clock = FakeClock()
    manager, _, _ = make_manager(clock, retry_backoff_ms=100)
    peer = FakeTelemetryPeer()
    peer.get_error_code = Errors.InvalidRequestError.errno
    request = manager.next_request()
    manager.handle_response(peer.send_and_receive(0, request))
    assert manager.state == ClientTelemetryManager.SUBSCRIPTION_NEEDED
    assert manager.time_to_next_update_ms() == 100
    peer.get_error_code = 0
    clock.advance(100)
    subscribe(manager, peer)


def wait_for(condition, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return False


def test_reporter_thread_end_to_end():
    metrics, _, _ = make_metrics()
    peer = FakeTelemetryPeer()
    peer.push_interval_ms = 50
    peer.delta_temporality = True
    peer.fail_next = 2  # transport failures must not kill the reporter
    reporter = ClientTelemetryReporter(peer, metrics, client_id='it-client',
                                       retry_backoff_ms=10,
                                       request_timeout_ms=1000)
    reporter.start()
    try:
        assert wait_for(lambda: len(peer.decoded_pushes) >= 2)
        first_request, first_decoded = peer.decoded_pushes[0]
        assert first_request.terminating is False
        counter = decoded_metrics(first_decoded)['producer.metrics.request.total']
        assert counter['sum']['data_points'][0]['as_int'] == 5
    finally:
        reporter.close(timeout_ms=10000)
    assert reporter.manager.terminated
    assert peer.decoded_pushes[-1][0].terminating is True
