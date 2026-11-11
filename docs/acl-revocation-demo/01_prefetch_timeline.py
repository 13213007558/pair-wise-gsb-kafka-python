"""
01_prefetch_timeline.py
=======================

Scenario: the broker's READ ACL for this principal is revoked *after* one
FetchResponse has already crossed the network into the client.

What the run proves (timestamps are printed in causal order):

  t1  first FetchRequest  -> broker still authorized, returns 3 records
  t2  those bytes land in Fetcher._completed_fetches (client-side buffer)
  t3  poll() #1 drains + *deserializes* the buffered batch -> 3 records
  t4  operator removes the READ ACL (we flip the scripted fake broker)
  t5  poll() #2 issues the *next* FetchRequest (buffer was empty)
  t6  broker answers TOPIC_AUTHORIZATION_FAILED (errno 29)
  t7  the error is raised only when poll() parses that response

Key conclusion: the records delivered by t2 were already in process memory.
No remote ACL change can reach back and delete them; the ACL only gates the
next FetchRequest the broker is willing to answer.

Run:  .venv/bin/python docs/acl-revocation-demo/01_prefetch_timeline.py
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from kafka import KafkaConsumer
from kafka.protocol.consumer import (
    FetchRequest, ListOffsetsRequest, OffsetForLeaderEpochRequest)
from kafka.serializer import Deserializer
import kafka.errors as Errors

from fakebroker_util import (
    make_broker, fetch_ok, fetch_denied, list_offsets_response,
    ofle_response, TP)

CLOCK = [0.0]


def ts(label):
    CLOCK[0] += 1.0
    print('  t%-2d %s' % (CLOCK[0], label))


class TracingDecoder(Deserializer):
    """Deserialization happens lazily, inside poll(), not on network receipt."""

    def deserialize(self, topic, headers, raw):
        ts('deserialize value: %r -> str' % (raw,))
        return raw.decode('utf-8')


def main():
    broker = make_broker()
    fetch_count = {'n': 0}

    def fetch_handler(api_key, api_version, correlation_id, request_bytes):
        fetch_count['n'] += 1
        n = fetch_count['n']
        if n == 1:
            ts('fake broker: FetchRequest #%d authorized -> 3 records' % n)
            return fetch_ok([b'evt-0', b'evt-1', b'evt-2'])
        ts('fake broker: FetchRequest #%d ACL denied -> errno 29' % n)
        return fetch_denied()

    broker.respond_always(ListOffsetsRequest, list_offsets_response(0))
    broker.respond_always(OffsetForLeaderEpochRequest, ofle_response())
    # respond_always: the same stateful handler answers every FetchRequest,
    # flipping from records to TOPIC_AUTHORIZATION_FAILED after the first one.
    broker.respond_always(FetchRequest, fetch_handler)

    consumer = KafkaConsumer(
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=(3, 5),
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        value_deserializer=TracingDecoder(),
        kafka_client=broker.client_factory(),
        request_timeout_ms=4000,
        default_api_timeout_ms=4000,
    )
    consumer.assign([TP])

    print('\n== phase A: healthy broker, one batch is prefetched ==')
    ts('call poll() #1')
    batch = consumer.poll(timeout_ms=3000)
    for records in batch.values():
        for r in records:
            ts('application received offset=%d value=%s' % (r.offset, r.value))

    fetcher = consumer._fetcher
    print('\n-- buffer state after poll #1 --')
    print('  _completed_fetches (raw wire batches queued): %d'
          % len(fetcher._completed_fetches))
    print('  in-flight fetch futures: %s' % fetcher.in_flight_fetches())

    print('\n== phase B: READ ACL revoked on the broker ==')
    ts('operator runs: kafka-acls --remove --allow-principal ... --operation Read')
    # Nothing is touched in the client process. The "revocation" is purely the
    # server-side script flip that fetch_handler already implements from #2 on.

    print('\n== phase C: next poll must now cross the revoked ACL ==')
    ts('call poll() #2')
    try:
        consumer.poll(timeout_ms=3000)
    except Errors.TopicAuthorizationFailedError as exc:
        ts('poll() #2 raised %s: %s'
           % (type(exc).__name__, sorted(exc.args[0]) if exc.args else exc))
    else:
        ts('ERROR: expected TopicAuthorizationFailedError')

    print('\n-- buffer state after the denied poll --')
    print('  _completed_fetches: %d (denied partition carries no records)'
          % len(fetcher._completed_fetches))
    consumer.close()
    print('\nRESULT: prefetched bytes were delivered before the revocation and'
          '\n        could not be recalled; the ACL only rejected the next fetch.')


if __name__ == '__main__':
    main()
