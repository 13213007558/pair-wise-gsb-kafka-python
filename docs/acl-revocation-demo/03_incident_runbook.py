"""
03_incident_runbook.py
======================

Reference shape of an ACL-revocation runbook for a security-event processing
service. It is deliberately a library-level pattern plus a runnable mock
demonstration, not a claim about what a broker ACL can retroactively delete.

Layering of guarantees (the important part)
-------------------------------------------
  * Broker ACL = a per-request admission decision on FUTURE FetchRequests.
    It cannot reach into a running consumer process.
  * Therefore containment of data already fetched is a *client-side* control:
    pause / reassign / close, and, for records already returned by poll(),
    application-level handling (quarantine, no downstream writes).
  * Anything already materialized (logs, metrics, caches, downstream sinks,
    heap memory) is outside Kafka's control plane. Revocation does not purge
    those surfaces; incident response must.

The demo runs two phases against the scripted fake broker:
  phase 1 normal processing until a batch is in flight;
  phase 2 simulated revocation -> TopicAuthorizationFailedError -> runbook.

Run:  .venv/bin/python docs/acl-revocation-demo/03_incident_runbook.py
"""
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
logging.disable(logging.CRITICAL)

from kafka import KafkaConsumer
from kafka.protocol.consumer import (
    FetchRequest, ListOffsetsRequest, OffsetForLeaderEpochRequest)
import kafka.errors as Errors

from fakebroker_util import (
    make_broker, fetch_ok, fetch_denied, list_offsets_response,
    ofle_response, TP, TOPIC)


class Quarantine:
    """Application-side sink for records already delivered to this process."""

    def __init__(self):
        self.items = []

    def hold(self, record, reason):
        self.items.append((record.offset, record.value, reason))


class SecurityEventService:
    """Minimal poll loop with an explicit revocation/containment path."""

    def __init__(self, consumer, quarantine):
        self.consumer = consumer
        self.quarantine = quarantine
        self.processed_offsets = []
        self.stopped = False

    def revoke_and_contain(self, revoked_tps):
        """Client-side containment that actually stops residual delivery.

        Order matters:
          1. pause() first to make sure no NEW FetchRequest is issued while
             we reassign (defense in depth; cheap and non-destructive);
          2. reassign the revoked tps away so buffered-but-unreturned records
             can no longer be returned by poll();
          3. record the current positions for the incident timeline;
          4. leave close() as the stronger option when the whole principal is
             untrusted (this demo calls it explicitly at the end).
        """
        self.consumer.pause(*revoked_tps)
        still_assigned = [tp for tp in self.consumer.assignment()
                          if tp not in set(revoked_tps)]
        positions = {
            tp: self.consumer.position(tp) for tp in revoked_tps
            if tp in self.consumer.assignment()}
        # Manual assignment mode: replace the assignment without the revoked tps.
        # In subscribe()/group mode the equivalent is a rebalance triggered by
        # removing the topic from the subscription.
        self.consumer.assign(still_assigned)
        return positions

    def run_once(self):
        try:
            batch = self.consumer.poll(timeout_ms=3000)
        except Errors.TopicAuthorizationFailedError as exc:
            # The broker has started rejecting FetchRequests. Anything poll()
            # returned in earlier calls is already in this process and must be
            # handled here; the ACL itself will not pull it back.
            self.stopped = True
            return 'DENIED:%s' % sorted(exc.args[0])

        for tp, records in batch.items():
            for record in records:
                # Normal processing would write to downstream systems. On an
                # active incident we instead quarantine records already in hand
                # so no new side effects are produced from residual data.
                self.quarantine.hold(record, 'in-flight at revocation')
                self.processed_offsets.append(record.offset)
        return 'OK:%d' % sum(len(v) for v in batch.values())


def main():
    broker = make_broker()
    fetch_state = {'n': 0, 'revoked': False}

    def fetch_handler(a, b, c, d):
        fetch_state['n'] += 1
        if fetch_state['revoked']:
            return fetch_denied()
        # Two healthy batches of 5 records each.
        return fetch_ok([b'evt-%d' % i for i in range(5)],
                        offset=(fetch_state['n'] - 1) * 5)

    broker.respond_always(ListOffsetsRequest, list_offsets_response(0))
    broker.respond_always(OffsetForLeaderEpochRequest, ofle_response())
    broker.respond_always(FetchRequest, fetch_handler)

    consumer = KafkaConsumer(
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=(3, 5), group_id=None, enable_auto_commit=False,
        auto_offset_reset='earliest', max_poll_records=5,
        kafka_client=broker.client_factory(),
        request_timeout_ms=4000, default_api_timeout_ms=4000)
    consumer.assign([TP])
    quarantine = Quarantine()
    svc = SecurityEventService(consumer, quarantine)

    print('== phase 1: normal operation ==')
    print('  loop 1 ->', svc.run_once())

    print('\n== phase 2: incident -> broker ACL removed ==')
    print('  server-side action: kafka-acls --remove (READ on %s for principal)'
          % TOPIC)
    fetch_state['revoked'] = True

    print('\n-- what the untouched client sees on its next poll --')
    print('  poll against revoked ACL ->', svc.run_once(),
          '(TopicAuthorizationFailedError surfaced inside poll)')

    print('\n-- client-side runbook --')
    # 1) records already returned by poll() are ours to deal with.
    print('  quarantined before containment: %d records (already in process)'
          % len(quarantine.items))
    # 2) stop residual delivery of buffered data and new fetches.
    positions = svc.revoke_and_contain([TP])
    print('  positions at containment: %s' % positions)
    print('  assignment after containment: %s' % consumer.assignment())
    print('  further poll returns no records: %s'
          % svc.run_once())

    # 3) strongest containment when the whole credential is untrusted.
    consumer.close()
    print('  consumer closed; no further fetches possible from this process.')

    print('\n== residual-risk boundary ==')
    print('  broker ACL guarantees : future FetchRequests are rejected (errno 29)')
    print('  client containment    : buffered/unreturned records stop here')
    print('  NOT recoverable remotely:')
    for offset, value, reason in quarantine.items:
        print('    offset=%d value=%s (%s)' % (offset, value.decode(), reason))
    print('  plus any copies already written to logs, caches, metrics, or'
          '\n  downstream sinks before containment -- purge those separately.')


if __name__ == '__main__':
    main()
