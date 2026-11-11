"""
04_inflight_revocation.py
========================

The narrowest timing question: if the READ ACL is removed *while a FetchRequest
is already in flight*, what does the client get?

Kafka authorization is evaluated when the broker processes the request. A
request that was admitted and whose records were already serialized into the
response before the ACL change is committed can still return data. The mock
below makes that window deterministic by delaying the broker's answer.

Two sub-cases are shown:
  (a) the request completes BEFORE the ACL update is effective -> records;
  (b) the ACL update is effective when the delayed request is answered ->
      TOPIC_AUTHORIZATION_FAILED.

Either way the decision is made server-side per request; the client cannot
distinguish "revoked just now" from "never authorized" except via the error.

Run:  .venv/bin/python docs/acl-revocation-demo/04_inflight_revocation.py
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
    ofle_response, TP)


def build_consumer(broker, first_handler, thereafter):
    broker.respond_always(ListOffsetsRequest, list_offsets_response(0))
    broker.respond_always(OffsetForLeaderEpochRequest, ofle_response())
    # One-shot delayed response for the in-flight request under test...
    broker.respond_fn(FetchRequest, first_handler)
    # ...plus a persistent fallback for every FetchRequest the poll loop
    # issues afterwards.
    broker.respond_always(FetchRequest, lambda a, b, c, d: thereafter())
    return KafkaConsumer(
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=(3, 5), group_id=None, enable_auto_commit=False,
        auto_offset_reset='earliest', max_poll_records=10,
        kafka_client=broker.client_factory(),
        request_timeout_ms=4000, default_api_timeout_ms=4000)


def run(label, response_factory, expect):
    print('\n-- %s --' % label)
    broker = make_broker()

    async def delayed_handler(api_key, api_version, correlation_id, request_bytes):
        # Hold the request in flight; meanwhile the "operator" flips the ACL.
        net = next(iter(broker._transports))._net
        await net.sleep(0.3)
        print('  broker resolves the in-flight request -> %s' % expect)
        return response_factory()

    # After the in-flight request, keep denying: once the ACL is gone it stays
    # gone for this incident, so the demo's end state is the same in both cases.
    consumer = build_consumer(broker, delayed_handler, fetch_denied)
    consumer.assign([TP])
    print('  FetchRequest sent; ACL change lands while it is in flight')
    try:
        batch = consumer.poll(timeout_ms=3000)
        n = sum(len(v) for v in batch.values())
        print('  poll() returned %d records (request was admitted pre-revocation)'
              % n)
        assert expect == 'records' and n == 2
    except Errors.TopicAuthorizationFailedError:
        print('  poll() raised TopicAuthorizationFailedError'
              ' (ACL effective when the request was answered)')
        assert expect == 'denied'
    consumer.close()


def main():
    # (a) admitted before revocation: the delayed response still carries data.
    run('case a: in-flight request admitted before ACL removal',
        lambda: fetch_ok([b'inflight-0', b'inflight-1']), 'records')
    # (b) ACL effective before the response is produced: error code 29.
    run('case b: ACL removal effective before the response is produced',
        fetch_denied, 'denied')
    print('\nCONCLUSION: authorization is a per-request server-side check with'
          '\nno retroactive scope; data from an already-admitted request can'
          '\nstill arrive, and nothing already in the client is ever recalled.')


if __name__ == '__main__':
    main()
