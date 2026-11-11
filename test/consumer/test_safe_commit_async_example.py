# pylint: skip-file
"""Executable spec for examples/safe_commit_async.py.

The example teaches the application-side ownership guard for
``commit_async`` callbacks. These tests drive its guard logic against a
lightweight fake consumer (no network) so the recommended pattern is
regression-protected alongside the coordinator-level reproduction tests in
test_coordinator.py.
"""
import os
import sys

import pytest

from kafka.structs import ConsumerGroupMetadata, OffsetAndMetadata, TopicPartition

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'examples'))
import safe_commit_async as example  # noqa: E402


class FakeConsumer:
    """Minimal stand-in for the KafkaConsumer surface the example uses."""

    def __init__(self, generation_id, assignment):
        self._generation_id = generation_id
        self._assignment = set(assignment)
        self.commits = []

    def group_metadata(self):
        return ConsumerGroupMetadata(
            group_id='g', generation_id=self._generation_id,
            member_id='m', state='stable')

    def assignment(self):
        return set(self._assignment)

    def commit_async(self, offsets, callback=None):
        self.commits.append((dict(offsets), callback))

    def commit(self, offsets, timeout_ms=None):
        self.commits.append(('sync', dict(offsets)))


def test_callback_advances_state_while_ownership_unchanged():
    tp = TopicPartition('t', 0)
    consumer = FakeConsumer(generation_id=1, assignment={tp})
    committer = example.OwnershipGuardedCommitter(consumer)

    committer.commit_processed({tp: OffsetAndMetadata(10, '', -1)})
    assert len(consumer.commits) == 1
    offsets, callback = consumer.commits[0]

    # Same generation, still assigned: the success is attributable.
    callback(offsets, None)
    assert committer.processed_offsets == {tp: 10}


def test_callback_ignored_after_generation_bump():
    tp = TopicPartition('t', 0)
    consumer = FakeConsumer(generation_id=1, assignment={tp})
    committer = example.OwnershipGuardedCommitter(consumer)

    committer.commit_processed({tp: OffsetAndMetadata(10, '', -1)})
    offsets, callback = consumer.commits[0]

    # Rebalance establishes a new generation before the response arrives.
    consumer._generation_id = 2
    consumer._assignment = set()
    callback(offsets, None)

    # The stale success must not mark anything done for the new owner.
    assert committer.processed_offsets == {}


def test_callback_ignored_after_partition_revoked_same_generation():
    tp = TopicPartition('t', 0)
    consumer = FakeConsumer(generation_id=1, assignment={tp})
    committer = example.OwnershipGuardedCommitter(consumer)

    committer.commit_processed({tp: OffsetAndMetadata(10, '', -1)})
    offsets, callback = consumer.commits[0]

    # Even without a generation change (cooperative rebalance), losing the
    # assignment is enough to reject the stale callback.
    consumer._assignment = set()
    callback(offsets, None)
    assert committer.processed_offsets == {}


def test_error_callback_never_advances_state():
    tp = TopicPartition('t', 0)
    consumer = FakeConsumer(generation_id=1, assignment={tp})
    committer = example.OwnershipGuardedCommitter(consumer)

    committer.commit_processed({tp: OffsetAndMetadata(10, '', -1)})
    offsets, callback = consumer.commits[0]
    callback(offsets, RuntimeError('broker gone'))
    assert committer.processed_offsets == {}


def test_commit_processed_rejects_unassigned_partitions():
    tp0 = TopicPartition('t', 0)
    tp1 = TopicPartition('t', 1)
    consumer = FakeConsumer(generation_id=1, assignment={tp0})
    committer = example.OwnershipGuardedCommitter(consumer)

    with pytest.raises(RuntimeError):
        committer.commit_processed({tp1: OffsetAndMetadata(10, '', -1)})
    assert consumer.commits == []


def test_revocation_listener_flushes_synchronously():
    tp = TopicPartition('t', 0)
    consumer = FakeConsumer(generation_id=1, assignment={tp})
    committer = example.OwnershipGuardedCommitter(consumer)
    # Pretend an async callback already recorded processed state.
    committer.commit_processed({tp: OffsetAndMetadata(10, '', -1)})
    offsets, callback = consumer.commits[0]
    callback(offsets, None)

    listener = example.RevocationFlushListener(committer)
    listener.on_partitions_revoked({tp})

    # The revocation flush uses the blocking commit, not commit_async.
    assert consumer.commits[-1][0] == 'sync'
    assert consumer.commits[-1][1] == {tp: OffsetAndMetadata(10, '', -1)}
