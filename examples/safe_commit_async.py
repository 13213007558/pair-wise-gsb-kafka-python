"""Safe use of ``KafkaConsumer.commit_async`` across rebalances.

Why this exists
---------------
``commit_async`` returns before the broker responds. A rebalance can land in
that window: the partition you committed may already belong to another member
by the time your success callback fires. kafka-python invokes the callback
exactly once with the broker-side result, but it does **not** attach the
generation that sent the request, and it never updates your assignment or
fetch positions from the callback. A callback that unconditionally marks work
"done" on success can therefore attribute an old owner's commit to work a new
owner has not processed yet.

The broker may legitimately answer an old-generation ``OffsetCommitRequest``
with success (the request was valid when it was processed), so fencing cannot
rely on seeing an ``IllegalGenerationError`` every time. The robust guard is
application-side ownership tracking:

1. snapshot the generation and owned partitions *when sending* the commit;
2. in the callback, compare that snapshot against the consumer's *current*
   generation and assignment before acting on the result.

The same guard works when the group coordinator fails over to another broker:
the response you receive still belongs to the request you sent, while
``consumer.assignment()`` / the coordinator generation describe live state.

Run with a reachable cluster, e.g.::

    python examples/safe_commit_async.py --bootstrap localhost:9092 --topic t
"""

import argparse
import logging

from kafka import (
    KafkaConsumer, OffsetAndMetadata, TopicPartition,
    ConsumerRebalanceListener,
)

log = logging.getLogger(__name__)


class OwnershipGuardedCommitter:
    """Commit offsets without letting stale callbacks cross ownership.

    Every in-flight async commit is tagged with the generation and assignment
    captured at send time. Callbacks only take effect when the consumer is
    still in that generation and still owns every committed partition.
    """

    def __init__(self, consumer):
        self.consumer = consumer
        # Application-side "processed" watermark. In a real service this would
        # be your work-state store; it must only advance for the current owner.
        self._processed_offsets = {}

    def _current_generation(self):
        # group_metadata() carries the live generation id even mid-rebalance;
        # it is None-ish defaults (-1 / '') before the first join.
        return self.consumer.group_metadata().generation_id

    def commit_processed(self, offsets):
        """Commit {TopicPartition: OffsetAndMetadata} asynchronously.

        The callback ignores results that arrive after an ownership change.
        """
        snapshot_generation = self._current_generation()
        snapshot_assignment = set(self.consumer.assignment())
        unknown = set(offsets) - snapshot_assignment
        if unknown:
            # Never commit partitions this consumer does not currently own.
            raise RuntimeError(
                'refusing to commit unassigned partitions: %s' % (unknown,))

        def on_commit(committed, res_or_exc):
            if isinstance(res_or_exc, Exception):
                # The commit failed. Do not advance local state; the next
                # poll loop or a synchronous commit can retry.
                log.warning('offset commit failed: %s', res_or_exc)
                return

            live_generation = self._current_generation()
            live_assignment = self.consumer.assignment()
            if live_generation != snapshot_generation:
                log.info(
                    'dropping commit callback: generation moved %s -> %s',
                    snapshot_generation, live_generation)
                return
            for tp, offset_and_meta in committed.items():
                if tp not in live_assignment:
                    log.info(
                        'dropping commit callback: %s no longer assigned', tp)
                    continue
                # Same generation, still the owner: attributing this result
                # cannot overwrite another member's work.
                self._processed_offsets[tp] = offset_and_meta.offset

        return self.consumer.commit_async(offsets, callback=on_commit)

    @property
    def processed_offsets(self):
        return dict(self._processed_offsets)


class RevocationFlushListener(ConsumerRebalanceListener):
    """Flush in-flight work and commit synchronously while still the owner.

    ``on_partitions_revoked`` runs before the partition is handed to another
    member, so a *synchronous* commit here is fenced by the current generation
    and is the correct place for a final, authoritative offset flush.
    """

    def __init__(self, committer):
        self.committer = committer
        self.consumer = committer.consumer

    def on_partitions_revoked(self, revoked):
        log.info('partitions revoked: %s; flushing synchronously', revoked)
        offsets = {
            tp: OffsetAndMetadata(self.committer.processed_offsets.get(tp, 0),
                                  '', -1)
            for tp in revoked
        }
        # commit() blocks and surfaces failures: no stale-callback window.
        if offsets:
            self.consumer.commit(offsets)

    def on_partitions_assigned(self, assigned):
        log.info('partitions assigned: %s', assigned)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--topic', default='safe-commit-demo')
    parser.add_argument('--group', default='safe-commit-demo-group')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
    )
    committer = OwnershipGuardedCommitter(consumer)
    # Registering the listener is optional for the callback guard itself, but
    # together they give both the steady-state and the revocation paths.
    consumer.subscribe(topics=[args.topic],
                       listener=RevocationFlushListener(committer))

    try:
        for message in consumer:
            tp = TopicPartition(message.topic, message.partition)
            # ... process message ...
            next_offset = message.offset + 1
            committer.commit_processed({
                tp: OffsetAndMetadata(next_offset, '', -1),
            })
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
