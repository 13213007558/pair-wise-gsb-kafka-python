"""Background KIP-714 client telemetry reporter.

Runs the ClientTelemetryManager loop on a daemon thread so telemetry never
blocks the produce/consume path: requests are sent with
client.send_and_receive on the least-loaded node, and every failure is
caught, logged, and fed back to the manager as a retry-with-backoff.
"""

import logging
import threading

import kafka.errors as Errors

from .emitter import DEFAULT_MAX_PAYLOAD_BYTES, MetricsEmitter
from .manager import ClientTelemetryManager


log = logging.getLogger(__name__)


class ClientTelemetryReporter:
    """Periodically pushes client metrics to the broker (KIP-714).

    Arguments:
        client: a KafkaNetClient-compatible client used to send requests.
        metrics (Metrics): the metrics registry to report.
        client_id (str): client id reported as a resource attribute.
        max_payload_bytes (int): maximum compressed payload size per push.
        retry_backoff_ms (int): backoff after failed requests.
        request_timeout_ms (int): timeout for individual telemetry requests.
        time_ms (callable, optional): injectable clock (milliseconds).
    """

    def __init__(self, client, metrics, client_id='',
                 max_payload_bytes=DEFAULT_MAX_PAYLOAD_BYTES,
                 retry_backoff_ms=100, request_timeout_ms=30000, time_ms=None):
        self._client = client
        self._request_timeout_ms = request_timeout_ms
        self.emitter = MetricsEmitter(metrics, client_id=client_id,
                                      max_payload_bytes=max_payload_bytes,
                                      time_ms=time_ms)
        self.manager = ClientTelemetryManager(self.emitter,
                                              retry_backoff_ms=retry_backoff_ms,
                                              time_ms=time_ms)
        self._wake = threading.Event()
        self._thread = None

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name='kafka-client-telemetry', daemon=True)
        self._thread.start()

    def close(self, timeout_ms=30000):
        """Trigger a final terminating push and stop the reporter thread."""
        self.manager.initiate_close()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(max(0, timeout_ms) / 1000.0)
            self._thread = None

    def _run(self):
        while not self.manager.terminated:
            wait_ms = self.manager.time_to_next_update_ms()
            if wait_ms is None:
                break
            self._wake.wait(wait_ms / 1000.0)
            self._wake.clear()
            if self.manager.terminated:
                break
            request = self.manager.next_request()
            if request is None:
                continue
            try:
                node_id = self._client.least_loaded_node(bootstrap_fallback=True)
                if node_id is None:
                    raise Errors.KafkaConnectionError(
                        'No broker available for telemetry request')
                response = self._client.send_and_receive(
                    node_id, request, timeout_ms=self._request_timeout_ms)
                self.manager.handle_response(response)
            except Exception as error:  # pylint: disable=broad-except
                # Telemetry must never interfere with produce/consume.
                log.debug('Telemetry request failed: %s', error)
                self.manager.handle_request_failure(error)
