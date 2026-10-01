"""KIP-714 client telemetry state machine.

Drives the GetTelemetrySubscriptions / PushTelemetry request cycle:

    subscription_needed --> push_needed --> terminating_push_needed --> terminated

The manager is transport-agnostic: the caller asks for the next request when
time_to_next_update_ms() reaches zero and feeds responses (or failures) back.
All broker-side problems (throttling, unknown subscriptions, oversized
payloads, request failures) are handled internally with backoff so telemetry
can never block or fail the produce/consume path.
"""
import logging
import threading
import time

import kafka.errors as Errors
from kafka.protocol.telemetry import (
    GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsResponse,
    PushTelemetryRequest,
    PushTelemetryResponse,
)
from .emitter import TelemetrySubscription

log = logging.getLogger(__name__)


class ClientTelemetryManager:
    """Tracks telemetry subscription state and builds telemetry requests."""

    SUBSCRIPTION_NEEDED = 'subscription_needed'
    PUSH_NEEDED = 'push_needed'
    TERMINATING_PUSH_NEEDED = 'terminating_push_needed'
    TERMINATED = 'terminated'

    def __init__(self, emitter, retry_backoff_ms=100, time_ms=None):
        self._emitter = emitter
        self._retry_backoff_ms = retry_backoff_ms
        self._time_ms = time_ms or (lambda: time.monotonic() * 1000)
        self._lock = threading.RLock()
        self._state = self.SUBSCRIPTION_NEEDED
        self._client_instance_id = None
        self._subscription = None
        self._next_update_ms = 0

    @property
    def state(self):
        return self._state

    @property
    def terminated(self):
        return self._state == self.TERMINATED

    @property
    def subscription(self):
        return self._subscription

    def _now(self):
        return self._time_ms()

    def time_to_next_update_ms(self):
        with self._lock:
            if self._state == self.TERMINATED:
                return None
            return max(0, self._next_update_ms - self._now())

    def next_request(self):
        """Return the next telemetry request to send, or None if it is not
        yet time (or a metrics payload was dropped for exceeding the maximum
        payload size)."""
        with self._lock:
            now = self._now()
            if self._state == self.TERMINATED or now < self._next_update_ms:
                return None
            if self._state == self.SUBSCRIPTION_NEEDED:
                return GetTelemetrySubscriptionsRequest[0](
                    client_instance_id=self._client_instance_id)
            if self._state in (self.PUSH_NEEDED, self.TERMINATING_PUSH_NEEDED):
                collected = self._emitter.collect()
                if collected is None:
                    # Payload dropped (e.g. too large). Do not retry or split
                    # it -- that would double-count delta temporality data.
                    self._next_update_ms = now + self._subscription.push_interval_ms
                    if self._state == self.TERMINATING_PUSH_NEEDED:
                        self._state = self.TERMINATED
                    return None
                payload, compression_type = collected
                return PushTelemetryRequest[0](
                    client_instance_id=self._client_instance_id,
                    subscription_id=self._subscription.subscription_id,
                    terminating=self._state == self.TERMINATING_PUSH_NEEDED,
                    compression_type=compression_type,
                    metrics=payload)
            return None

    def handle_response(self, response):
        with self._lock:
            if self._state == self.TERMINATED:
                return
            if isinstance(response, GetTelemetrySubscriptionsResponse):
                self._handle_subscription_response(response)
            elif isinstance(response, PushTelemetryResponse):
                self._handle_push_response(response)
            else:
                raise ValueError('Unexpected telemetry response: %r' % (response,))

    def handle_request_failure(self, error=None):
        """Record a transport-level failure; retry after a backoff. Never
        raises -- telemetry must not interfere with produce/consume."""
        with self._lock:
            if self._state == self.TERMINATED:
                return
            log.debug('Telemetry request failed, retrying in %d ms: %s',
                      self._retry_backoff_ms, error)
            self._next_update_ms = self._now() + self._retry_backoff_ms

    def initiate_close(self):
        """Transition towards a final terminating push (if subscribed)."""
        with self._lock:
            if self._state == self.PUSH_NEEDED:
                self._state = self.TERMINATING_PUSH_NEEDED
                self._next_update_ms = 0
            elif self._state == self.SUBSCRIPTION_NEEDED:
                self._state = self.TERMINATED

    def _handle_subscription_response(self, response):
        now = self._now()
        error = Errors.for_code(response.error_code)
        if error is not Errors.NoError:
            log.warning('GetTelemetrySubscriptions failed: %s; retrying in %d ms',
                        error.__name__, self._retry_backoff_ms)
            self._next_update_ms = now + self._retry_backoff_ms
            return
        subscription = TelemetrySubscription(
            client_instance_id=response.client_instance_id,
            subscription_id=response.subscription_id,
            push_interval_ms=response.push_interval_ms,
            delta_temporality=response.delta_temporality,
            accepted_compression_types=list(response.accepted_compression_types),
            requested_metrics=list(response.requested_metrics))
        self._client_instance_id = subscription.client_instance_id
        self._subscription = subscription
        self._emitter.update_subscription(subscription)
        if self._state == self.SUBSCRIPTION_NEEDED:
            self._state = self.PUSH_NEEDED
        self._next_update_ms = now + subscription.push_interval_ms + response.throttle_time_ms

    def _handle_push_response(self, response):
        now = self._now()
        error = Errors.for_code(response.error_code)
        if error is Errors.NoError:
            if self._state == self.TERMINATING_PUSH_NEEDED:
                self._state = self.TERMINATED
            else:
                self._next_update_ms = (now + response.throttle_time_ms
                                        + self._subscription.push_interval_ms)
        elif error is Errors.UnknownSubscriptionIdError:
            log.info('Broker no longer recognizes telemetry subscription %s;'
                     ' fetching a new subscription',
                     self._subscription.subscription_id if self._subscription else None)
            self._subscription = None
            self._emitter.reset()
            self._state = self.SUBSCRIPTION_NEEDED
            self._next_update_ms = now
        elif error is Errors.TelemetryTooLargeError:
            log.warning('Broker rejected telemetry push as too large;'
                        ' dropping the payload and continuing')
            if self._state == self.TERMINATING_PUSH_NEEDED:
                self._state = self.TERMINATED
            elif self._subscription is not None:
                self._next_update_ms = now + self._subscription.push_interval_ms
        else:
            log.warning('PushTelemetry failed: %s; retrying in %d ms',
                        error.__name__, self._retry_backoff_ms)
            self._next_update_ms = now + self._retry_backoff_ms
