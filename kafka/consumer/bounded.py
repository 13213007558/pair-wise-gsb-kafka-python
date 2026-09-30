"""Bounded read sessions over an explicit partition list.

A :class:`BoundedReadSession` serves offline-validation style reads that
want exactly the messages that existed when the call started -- unlike
:meth:`~kafka.KafkaConsumer.poll`, which keeps chasing newly appended
data forever.

The session works in two phases:

1. **Boundary capture** (:meth:`BoundedReadSession.begin`): for every
   partition in the explicit assignment, the current end offset is
   captured via ListOffsets. Under ``isolation_level='read_committed'``
   the broker returns the Last Stable Offset (LSO), i.e. the visible
   stable boundary; transactions still open at capture time are excluded
   and transactions that commit *after* capture do not extend it.
2. **Bounded read** (:meth:`BoundedReadSession.poll` /
   :meth:`BoundedReadSession.collect`): records are fetched from the
   caller-specified start offsets until each partition's fetch position
   reaches its captured boundary.

Boundaries are *offset ranges*, not expected record counts: log
compaction, retention deletion, and aborted-transaction filtering all
leave holes in the offset space, so a partition is complete when its
position reaches the boundary offset -- never when some number of
records has been seen. Records appended after capture (offset >=
boundary) are dropped from the results.

The session uses manual partition assignment only: it never joins a
consumer group, so no rebalance can move the assignment mid-read. If a
topic is deleted and recreated under the same name during the session
(KIP-516 topic id change), the session raises
:class:`TopicIdentityChangedError` instead of silently reading the new
log.
"""

import collections
import copy
import logging
import time

import kafka.errors as Errors
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.structs import TopicPartition
from kafka.util import Timer

log = logging.getLogger(__name__)


class TopicIdentityChangedError(Errors.KafkaError):
    """A topic's identity (KIP-516 topic id) changed mid-session.

    Raised when a topic is deleted and recreated under the same name
    while a bounded read is in flight. The session refuses to continue
    reading what is now a different log. Not raised on brokers older
    than Kafka 2.8, which do not report topic ids.
    """
    retriable = False
    invalid_metadata = True


PartitionBoundary = collections.namedtuple('PartitionBoundary',
    ['topic_partition', 'start_offset', 'end_offset'])
PartitionBoundary.__doc__ = """Captured read range for one partition.

``end_offset`` is the boundary captured at session start (log end, or
LSO under read_committed); the partition is read up to but not including
this offset.
"""

PartitionProgress = collections.namedtuple('PartitionProgress',
    ['topic_partition', 'start_offset', 'end_offset', 'position', 'complete'])
PartitionProgress.__doc__ = """Per-partition progress of a bounded read session.

``position`` is the partition's current fetch position (next offset to
be fetched); ``complete`` is True once it has reached ``end_offset``.
"""

BoundedReadResult = collections.namedtuple('BoundedReadResult',
    ['records', 'progress'])
BoundedReadResult.__doc__ = """Final outcome of :meth:`BoundedReadSession.collect`.

``records`` maps TopicPartition to the ConsumerRecords read within the
captured boundaries; ``progress`` maps TopicPartition to
:class:`PartitionProgress` with the actual position reached and the
boundary it was read against.
"""


class BoundedReadSession:
    """A finite read session over an explicit partition list.

    Reads each assigned partition from a caller-specified start offset
    up to the partition's end offset as captured when the session
    begins, then stops. See module docstring for the full semantics.

    Arguments:
        client (KafkaNetClient): client used for broker communication.
        offsets (dict): {TopicPartition: start_offset} -- the explicit
            assignment and the position each partition is read from.

    Keyword Arguments:
        isolation_level (str): 'read_uncommitted' (default) captures the
            log end as each partition's boundary; 'read_committed'
            captures the Last Stable Offset instead and filters aborted
            transactional batches while reading.
        timeout_ms (int): overall wall-clock budget for the session,
            spanning boundary capture and all polls. On expiry a
            KafkaTimeoutError is raised carrying the per-partition
            progress, so a partition that can never reach its boundary
            (e.g. its remaining range was deleted) cannot block the
            caller forever. None disables the budget. Default: 60000.

    Additional keyword arguments are forwarded to
    :class:`~kafka.consumer.fetcher.Fetcher` (e.g. ``max_poll_records``,
    ``fetch_max_wait_ms``, ``key_deserializer``, ``value_deserializer``,
    ``retry_backoff_ms``, ``metrics``).
    """

    DEFAULT_CONFIG = {
        'timeout_ms': 60000,
    }

    def __init__(self, client, offsets, **configs):
        if not offsets:
            raise ValueError('offsets must map at least one TopicPartition to a start offset')
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)
        extra_configs = set(configs).difference(Fetcher.DEFAULT_CONFIG)
        if extra_configs:
            raise Errors.KafkaConfigurationError('Unrecognized configs: %s' % (extra_configs,))

        self._starts = {}
        for tp, offset in offsets.items():
            if not isinstance(tp, TopicPartition):
                raise TypeError('offsets keys must be TopicPartition, got %r' % (tp,))
            if not isinstance(offset, int) or offset < 0:
                raise ValueError('start offset for %s must be a non-negative int, got %r' % (tp, offset))
            self._starts[tp] = offset

        self._client = client
        self._manager = client._manager
        self._net = self._manager._net
        self._cluster = self._manager.cluster

        # Manual assignment only: no consumer group, no rebalancing.
        # 'none' reset strategy: a start offset the log cannot serve
        # raises OffsetOutOfRangeError instead of silently resetting.
        self._subscriptions = SubscriptionState(offset_reset_strategy='none')
        self._subscriptions.assign_from_user(list(self._starts))
        self._cluster.set_topics([tp.topic for tp in self._starts])
        self._fetcher = Fetcher(client, self._subscriptions, **configs)

        self._timer = None
        self._timeout_ms = None
        self._boundaries = None
        self._topic_ids = None
        self._completed = set()
        self._closed = False

    # ------------------------------------------------------------------ #
    # Phase 1: boundary capture                                           #
    # ------------------------------------------------------------------ #

    def begin(self, timeout_ms=None):
        """Capture per-partition boundaries and arm the read positions.

        Issues ListOffsets (LATEST; LSO when the session runs with
        ``isolation_level='read_committed'``) for every assigned
        partition and records the cluster's topic ids for identity
        checking. Idempotent: subsequent calls return the originally
        captured boundaries.

        Arguments:
            timeout_ms (int, optional): overall session budget, overriding
                the configured ``timeout_ms``. The clock starts here.

        Returns:
            {TopicPartition: PartitionBoundary}

        Raises:
            KafkaTimeoutError: if boundaries cannot be captured within
                the session budget (e.g. a partition stays unavailable).
        """
        if self._closed:
            raise Errors.IllegalStateError('BoundedReadSession is closed')
        if self._boundaries is not None:
            return dict(self._boundaries)

        if timeout_ms is None:
            timeout_ms = self.config['timeout_ms']
        self._timeout_ms = timeout_ms
        self._timer = Timer(timeout_ms)

        budget = self._remaining_ms()
        ends = self._fetcher.end_offsets(
            list(self._starts),
            timeout_ms=None if budget == float('inf') else budget)

        # Capture topic identity after the ListOffsets round trip: by then
        # metadata for the assigned topics has necessarily been fetched.
        # Brokers older than Kafka 2.8 report no topic ids; identity
        # checking is skipped for those topics.
        self._topic_ids = {}
        for topic in {tp.topic for tp in self._starts}:
            self._topic_ids[topic] = self._cluster.topic_id(topic)

        self._boundaries = {}
        for tp, start in self._starts.items():
            end = ends[tp]
            self._boundaries[tp] = PartitionBoundary(tp, start, end)
            self._subscriptions.seek(tp, start)
            if start >= end:
                log.debug('Partition %s range [%d, %d) is empty at capture', tp, start, end)
                self._mark_complete(tp)
        return dict(self._boundaries)

    @property
    def boundaries(self):
        """Captured {TopicPartition: PartitionBoundary}, or None before begin()."""
        return None if self._boundaries is None else dict(self._boundaries)

    # ------------------------------------------------------------------ #
    # Phase 2: bounded read                                               #
    # ------------------------------------------------------------------ #

    @property
    def done(self):
        """True once every partition's position has reached its boundary."""
        return self._boundaries is not None and len(self._completed) == len(self._starts)

    @property
    def progress(self):
        """{TopicPartition: PartitionProgress} with actual positions and boundaries."""
        if self._boundaries is None:
            raise Errors.IllegalStateError('BoundedReadSession has not begun')
        progress = {}
        for tp, boundary in self._boundaries.items():
            position = self._subscriptions.assignment[tp].position
            progress[tp] = PartitionProgress(
                tp, boundary.start_offset, boundary.end_offset,
                None if position is None else position.offset,
                tp in self._completed)
        return progress

    def poll(self, max_records=None, timeout_ms=None):
        """Fetch one round of records, advancing towards the boundaries.

        Records at or beyond a partition's captured boundary (appended
        after the session began) are dropped from the result. Partitions
        that reach their boundary are paused and no longer fetched.

        Arguments:
            max_records (int, optional): cap on records returned this round.
            timeout_ms (int, optional): cap on this round's wait for
                in-flight fetches; the remaining session budget takes
                precedence when smaller.

        Returns:
            {TopicPartition: [ConsumerRecord]} -- possibly empty while the
            session is still incomplete (empty batches, filtered
            transactional data, or a partition waiting on its leader).

        Raises:
            KafkaTimeoutError: if the session budget expires before every
                partition reached its boundary. The session's ``progress``
                property reports how far each partition got.
            TopicIdentityChangedError: if an assigned topic was deleted
                and recreated (topic id change) since capture.
        """
        if self._closed:
            raise Errors.IllegalStateError('BoundedReadSession is closed')
        self.begin()
        if self.done:
            return {}

        self._check_topic_identity()
        budget = self._remaining_ms()
        if timeout_ms is not None:
            budget = min(budget, timeout_ms)
        records, idle = self._fetcher.fetch_records(
            max_records, update_offsets=True,
            timeout_ms=None if budget == float('inf') else budget)
        # Identity is re-checked after the fetch round: a metadata refresh
        # landing mid-round must not let records from a recreated topic
        # leak into the results.
        self._check_topic_identity()

        drained = self._drain_within_boundaries(records)
        if not self.done:
            self._raise_if_expired()
            if idle and not drained:
                # Nothing fetchable right now (e.g. leader unavailable and
                # metadata refresh in flight); avoid a busy loop.
                delay = self._fetcher.config['retry_backoff_ms'] / 1000.0
                if budget != float('inf'):
                    delay = min(delay, budget / 1000.0)
                if delay > 0:
                    time.sleep(delay)
        return drained

    def collect(self, max_records=None, timeout_ms=None):
        """Read every partition to its captured boundary and return the result.

        Loops :meth:`poll` until the session is done or the session
        budget expires.

        Arguments:
            max_records (int, optional): per-round cap on fetched records.
            timeout_ms (int, optional): per-round wait cap.

        Returns:
            BoundedReadResult: all in-boundary records plus the final
            per-partition progress (actual position vs. boundary).
        """
        records = collections.defaultdict(list)
        while not self.done:
            for tp, batch in self.poll(max_records=max_records, timeout_ms=timeout_ms).items():
                records[tp].extend(batch)
        return BoundedReadResult(dict(records), self.progress)

    def close(self):
        """Release fetch buffers. The session cannot be used afterwards."""
        if not self._closed:
            self._closed = True
            self._fetcher.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ------------------------------------------------------------------ #
    # Internals                                                           #
    # ------------------------------------------------------------------ #

    def _mark_complete(self, tp):
        self._completed.add(tp)
        # Pause so no further fetches are issued for this partition; any
        # of its buffered data beyond the boundary is discarded.
        self._subscriptions.pause(tp)

    def _drain_within_boundaries(self, records):
        """Drop records at/past the captured boundary; complete partitions
        whose position has reached it. Completion is offset-based: holes
        from compaction, deletion, or aborted-transaction filtering only
        move the position forward, they are never waited on."""
        drained = {}
        for tp, batch in records.items():
            end = self._boundaries[tp].end_offset
            kept = [record for record in batch if record.offset < end]
            if len(kept) < len(batch):
                log.debug('Discarding %d records at/past boundary %d for %s',
                          len(batch) - len(kept), end, tp)
            if kept:
                drained[tp] = kept
        for tp in self._starts:
            if tp in self._completed:
                continue
            position = self._subscriptions.assignment[tp].position
            if position is not None and position.offset >= self._boundaries[tp].end_offset:
                log.debug('Partition %s reached boundary %d at position %d',
                          tp, self._boundaries[tp].end_offset, position.offset)
                self._mark_complete(tp)
        return drained

    def _check_topic_identity(self):
        """Raise if a topic was recreated (topic id changed) since capture.

        Only positive mismatches abort: a topic id that is (or becomes)
        unknown -- pre-2.8 broker, or a metadata response without ids --
        cannot prove recreation and is left to the fetch/ListOffsets
        error paths.
        """
        for topic, captured in self._topic_ids.items():
            if captured is None:
                continue
            current = self._cluster.topic_id(topic)
            if current is not None and current != captured:
                raise TopicIdentityChangedError(
                    'Topic %s id changed from %s to %s (deleted and recreated?)'
                    ' during bounded read; refusing to read the new log'
                    % (topic, captured, current))

    def _remaining_ms(self):
        """Remaining session budget in ms; float('inf') when unbounded."""
        remaining = self._timer.timeout_ms
        return float('inf') if remaining is None else remaining

    def _raise_if_expired(self):
        if self._timer.expired:
            raise Errors.KafkaTimeoutError(
                'Bounded read session did not finish within %s ms;'
                ' per-partition progress: %s' % (self._timeout_ms, self.progress))
