"""Collect kafka-python Metrics into OpenTelemetry payloads for KIP-714.

The MetricsEmitter snapshots the Metrics registry, filters metrics by the
broker-requested name prefixes, maps cumulative stats (Total / Count) to
monotonic OTLP Sums and everything else to OTLP Gauges, applies the
broker-requested aggregation temporality (delta or cumulative), and
serializes/compresses the result respecting the broker's accepted compression
types and a maximum payload size.
"""

import logging
import math
import time

from kafka import codec
from kafka.metrics.stats import Count, Total
from kafka.version import __version__

from . import otel


log = logging.getLogger(__name__)

#: Default upper bound for a single (compressed) PushTelemetry metrics
#: payload. Payloads that would exceed this are dropped whole -- never split
#: into multiple pushes, which would mis-aggregate delta temporality data.
DEFAULT_MAX_PAYLOAD_BYTES = 1024 * 1024

COMPRESSION_NONE = 0
COMPRESSION_GZIP = 1
COMPRESSION_SNAPPY = 2
COMPRESSION_LZ4 = 3
COMPRESSION_ZSTD = 4

# Preferred order when the broker accepts several compression types.
_COMPRESSION_PREFERENCE = (
    (COMPRESSION_ZSTD, 'zstd'),
    (COMPRESSION_LZ4, 'lz4'),
    (COMPRESSION_SNAPPY, 'snappy'),
    (COMPRESSION_GZIP, 'gzip'),
)

SCOPE_NAME = 'kafka-python'


class TelemetrySubscription:
    """The telemetry subscription granted by the broker (KIP-714)."""
    __slots__ = ('client_instance_id', 'subscription_id', 'push_interval_ms',
                 'delta_temporality', 'accepted_compression_types',
                 'requested_metrics')

    def __init__(self, client_instance_id, subscription_id, push_interval_ms,
                 delta_temporality, accepted_compression_types,
                 requested_metrics):
        self.client_instance_id = client_instance_id
        self.subscription_id = subscription_id
        self.push_interval_ms = push_interval_ms
        self.delta_temporality = delta_temporality
        self.accepted_compression_types = list(accepted_compression_types)
        self.requested_metrics = list(requested_metrics)

    def __repr__(self):
        return ('TelemetrySubscription(client_instance_id=%s, subscription_id=%s,'
                ' push_interval_ms=%s, delta_temporality=%s)') % (
                    self.client_instance_id, self.subscription_id,
                    self.push_interval_ms, self.delta_temporality)


def otel_metric_name(metric_name):
    """Map a Kafka MetricName to its OTLP metric name.

    The group and name are joined with '.' and dashes are converted to '.',
    e.g. group 'producer-metrics' / name 'batch-size-avg' becomes
    'producer.metrics.batch.size.avg'.
    """
    return (metric_name.group + '.' + metric_name.name).replace('-', '.')


def _matches_prefixes(prefixes, name):
    for prefix in prefixes:
        if prefix == '*':
            return True
        if prefix.endswith('.'):
            if name.startswith(prefix):
                return True
        elif name == prefix or name.startswith(prefix + '.'):
            return True
    return False


def _is_counter(kafka_metric):
    return isinstance(kafka_metric.measurable, (Total, Count))


def _set_counter_value(point, value):
    if float(value).is_integer() and abs(value) < 2 ** 53:
        point['as_int'] = int(value)
    else:
        point['as_double'] = float(value)


class MetricsEmitter:
    """Builds OpenTelemetry metrics payloads from a Metrics registry.

    Arguments:
        metrics (Metrics): the metrics registry to sample.
        client_id (str): client id, reported as the 'client.id' resource
            attribute.
        max_payload_bytes (int): maximum size of a single compressed payload;
            larger payloads are dropped (never split).
        time_ms (callable, optional): clock returning milliseconds; injectable
            for tests.
    """

    def __init__(self, metrics, client_id='',
                 max_payload_bytes=DEFAULT_MAX_PAYLOAD_BYTES, time_ms=None):
        self._metrics = metrics
        self._client_id = client_id
        self._max_payload_bytes = max_payload_bytes
        self._time_ms = time_ms or (lambda: time.monotonic() * 1000)
        self._subscription = None
        self._subscription_start_ms = None
        self._last_collect_ms = None
        # MetricName -> last reported cumulative value (delta temporality)
        self._last_values = {}
        self._compression_type = COMPRESSION_NONE
        self._compress = None

    @property
    def subscription(self):
        return self._subscription

    def update_subscription(self, subscription):
        """Install a new subscription, resetting delta/cumulative baselines
        whenever the subscription id or client instance id changes (e.g. a
        new broker session)."""
        previous = self._subscription
        if (previous is None
                or previous.subscription_id != subscription.subscription_id
                or previous.client_instance_id != subscription.client_instance_id):
            self._last_values = {}
            self._subscription_start_ms = int(self._time_ms())
            self._last_collect_ms = None
        self._subscription = subscription
        self._compression_type, self._compress = self._select_compression(
            subscription.accepted_compression_types)

    def reset(self):
        """Drop the current subscription (e.g. broker reported it unknown)."""
        self._subscription = None
        self._last_values = {}
        self._last_collect_ms = None

    @staticmethod
    def _select_compression(accepted_compression_types):
        accepted = set(accepted_compression_types or ())
        for compression_type, name in _COMPRESSION_PREFERENCE:
            has_fn = getattr(codec, 'has_' + name, None)
            encode_fn = getattr(codec, name + '_encode', None)
            if (compression_type in accepted and has_fn is not None
                    and encode_fn is not None and has_fn()):
                return compression_type, encode_fn
        return COMPRESSION_NONE, None

    def collect(self):
        """Snapshot matching metrics into a (possibly compressed) payload.

        Returns:
            tuple[bytes, int] | None: (payload, compression_type), or None if
            there is no active subscription or the payload exceeded
            max_payload_bytes and was dropped. Delta-temporality baselines
            advance on every collection, so a dropped payload is lost rather
            than double-counted by a later retry.
        """
        subscription = self._subscription
        if subscription is None:
            return None
        now_ms = int(self._time_ms())
        if subscription.delta_temporality and self._last_collect_ms is not None:
            start_ms = self._last_collect_ms
        else:
            start_ms = self._subscription_start_ms

        collected = []
        current_values = {}
        # Copy the registry contents first: metrics may be registered
        # concurrently by the produce/consume path.
        registry_items = list(self._metrics.metrics.items())
        for metric_name, kafka_metric in sorted(registry_items,
                                                key=lambda item: str(item[0])):
            full_name = otel_metric_name(metric_name)
            if not _matches_prefixes(subscription.requested_metrics, full_name):
                continue
            try:
                value = kafka_metric.value(now_ms)
            except Exception:  # pylint: disable=broad-except
                log.debug('Skipping metric %s: value() failed', full_name, exc_info=True)
                continue
            if value is None or not math.isfinite(value):
                continue
            point = {
                'attributes': {str(k): str(v) for k, v in metric_name.tags.items()},
                'time_unix_nano': now_ms * 10 ** 6,
            }
            metric = {'name': full_name, 'description': metric_name.description or ''}
            if _is_counter(kafka_metric):
                current_values[metric_name] = value
                if subscription.delta_temporality:
                    last_value = self._last_values.get(metric_name)
                    reported = value if last_value is None else value - last_value
                else:
                    reported = value
                point['start_time_unix_nano'] = start_ms * 10 ** 6
                _set_counter_value(point, reported)
                metric['sum'] = {
                    'data_points': [point],
                    'aggregation_temporality': (
                        otel.AGGREGATION_TEMPORALITY_DELTA
                        if subscription.delta_temporality
                        else otel.AGGREGATION_TEMPORALITY_CUMULATIVE),
                    'is_monotonic': True,
                }
            else:
                point['as_double'] = float(value)
                metric['gauge'] = {'data_points': [point]}
            collected.append(metric)

        # Advance temporality baselines: each interval is reported exactly once.
        if subscription.delta_temporality:
            self._last_values.update(current_values)
            self._last_collect_ms = now_ms

        resource_attributes = {}
        if self._client_id:
            resource_attributes['client.id'] = self._client_id
        payload = otel.encode_metrics_data(
            collected,
            resource_attributes=resource_attributes,
            scope_name=SCOPE_NAME,
            scope_version=__version__)
        if self._compress is not None:
            payload = self._compress(payload)
        if len(payload) > self._max_payload_bytes:
            log.warning(
                'Dropping telemetry payload of %d bytes (limit %d); '
                'payloads are never split to avoid mis-aggregated deltas',
                len(payload), self._max_payload_bytes)
            return None
        return payload, self._compression_type
