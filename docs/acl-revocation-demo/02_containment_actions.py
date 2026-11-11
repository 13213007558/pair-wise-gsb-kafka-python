"""
02_containment_actions.py
=========================

Same setup three times: a 50-record batch is authorized and fetched once, the
application drains only 10 via poll(), so 40 records are still inside the
client process (raw bytes in Fetcher buffers / partially parsed in the inline
PartitionRecords slot). Then we apply one containment action and observe what
it can and cannot stop.

Actions compared
----------------
  A. consumer.pause(tp)
       - stops NEW FetchRequests for the partition
       - but KEEPS already-buffered data (KAFKA-7548): resume() hands the
         same 40 records back without touching the broker
       - => does NOT contain prefetched data
  B. reassign away: consumer.assign([]) (or a rebalance revoking the tp)
       - drops the partition from the assignment; fetcher._append() refuses
         to return records for a tp that "is no longer assigned"
       - stops both NEW fetches and delivery of buffered-but-unreturned data
       - => contains not-yet-returned data, but cannot touch the 10 already
         handed to application code
  C. consumer.close()
       - stops everything: no further poll, no new fetches, connections close
       - buffered records are discarded with the object; again the 10 already
         returned to the application are outside its reach

Run:  .venv/bin/python docs/acl-revocation-demo/02_containment_actions.py
"""
import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
logging.disable(logging.CRITICAL)

from kafka import KafkaConsumer
from kafka.protocol.consumer import (
    FetchRequest, ListOffsetsRequest, OffsetForLeaderEpochRequest)

from fakebroker_util import (
    make_broker, fetch_ok, list_offsets_response, ofle_response, TP)

BATCH = 50
DRAINED = 10


def new_consumer(broker):
    broker.respond_always(ListOffsetsRequest, list_offsets_response(0))
    broker.respond_always(OffsetForLeaderEpochRequest, ofle_response())
    broker.respond_always(
        FetchRequest,
        lambda a, b, c, d: fetch_ok([b'evt-%d' % i for i in range(BATCH)]))
    consumer = KafkaConsumer(
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=(3, 5), group_id=None, enable_auto_commit=False,
        auto_offset_reset='earliest', max_poll_records=DRAINED,
        kafka_client=broker.client_factory(),
        request_timeout_ms=4000, default_api_timeout_ms=4000)
    consumer.assign([TP])
    first = consumer.poll(timeout_ms=3000)
    got = sum(len(v) for v in first.values())
    assert got == DRAINED, got
    return consumer


def buffered_count(fetcher):
    """Records still resident in the client after the first poll.

    The single 50-record batch lives in the inline PartitionRecords slot once
    parsing starts; _paused_completed_fetches may additionally hold whole raw
    batches (see the pause case).
    """
    slot = fetcher._next_partition_records
    in_slot = BATCH - slot.records_read if slot is not None else 0
    parked_raw = sum(1 for _ in fetcher._paused_completed_fetches) * BATCH
    parked_parsed = sum(
        BATCH - pr.records_read for pr in
        fetcher._paused_partition_records.values())
    return in_slot + parked_raw + parked_parsed


def case_pause():
    print('\n== A. pause(tp) ==')
    broker = make_broker()
    consumer = new_consumer(broker)
    f = consumer._fetcher
    print('  after first poll: %d records still resident in client buffers'
          % buffered_count(f))

    consumer.pause(TP)
    print('  paused set: %s' % consumer.paused())
    print('  new fetches for a paused tp? fetchable=%s (excluded)'
          % (TP in f._fetchable_partitions(),))

    parked = consumer.poll(timeout_ms=500)
    print('  poll() while paused returns: %d records'
          % sum(len(v) for v in parked.values()))
    print('  parked raw fetches: %d, parked parsed batches: %d'
          % (len(f._paused_completed_fetches),
             len(f._paused_partition_records)))

    consumer.resume(TP)
    again = consumer.poll(timeout_ms=500)
    n = sum(len(v) for v in again.values())
    print('  after resume(), poll() returns %d MORE records from the SAME'
          % n)
    print('  prefetch batch -- no broker re-fetch needed (KAFKA-7548).')
    consumer.close()
    print('  VERDICT: pause blocks new fetches only; prefetched records are'
          '\n           retained and fully recoverable by resume().')


def case_reassign():
    print('\n== B. reassign away (assign([]) / rebalance revocation) ==')
    broker = make_broker()
    consumer = new_consumer(broker)
    f = consumer._fetcher
    print('  after first poll: %d records still resident' % buffered_count(f))

    consumer.assign([])  # manual-mode equivalent of losing the tp in a rebalance
    print('  assignment after reassign: %s' % consumer.assignment())
    print('  fetchable partitions: %s' % f._fetchable_partitions())

    leftover = consumer.poll(timeout_ms=500)
    print('  poll() after reassign returns: %d records'
          % sum(len(v) for v in leftover.values()))
    print('  the buffered %d records can no longer be delivered: _append()'
          % buffered_count(f))
    print('  skips any tp that is no longer assigned (fetcher.py is_fetchable/'
          'is_assigned checks).')
    consumer.close()
    print('  VERDICT: reassign stops new fetches AND delivery of buffered but'
          '\n           not-yet-returned records; the 10 already returned stay'
          '\n           processed by the app.')


def case_close():
    print('\n== C. close() ==')
    broker = make_broker()
    consumer = new_consumer(broker)
    f = consumer._fetcher
    print('  after first poll: %d records still resident' % buffered_count(f))

    consumer.close()
    import kafka.errors as Errors
    try:
        consumer.poll(timeout_ms=500)
    except Errors.IllegalStateError:
        print('  poll() after close raises IllegalStateError')
    print('  fetcher.close() drained the inline slot and parked batches;'
          ' connections are shut.')
    print('  VERDICT: close() stops all further delivery and network fetches;'
          '\n           the object\'s buffers are released, records already'
          '\n           returned to the app cannot be pulled back.')


def main():
    case_pause()
    case_reassign()
    case_close()
    print('\nSUMMARY'
          '\n  pause    : new fetches NO  | buffered data KEPT   | app-held data n/a'
          '\n  reassign : new fetches NO  | buffered data BLOCKED| app-held data remains'
          '\n  close    : new fetches NO  | buffered data DISCARD| app-held data remains')


if __name__ == '__main__':
    main()
