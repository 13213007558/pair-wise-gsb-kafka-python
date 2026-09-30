import collections
import logging
import time

import kafka.errors as Errors
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.protocol.consumer import OffsetSpec
from kafka.structs import TopicPartition


log = logging.getLogger(__name__)


BoundedPartitionResult = collections.namedtuple(
    'BoundedPartitionResult', ['progress', 'boundary'])
BoundedPartitionResult.__doc__ = """Finite read result for one partition.

``progress`` is the next offset that the session would fetch. ``boundary`` is
the exclusive end offset captured when the session started.
"""


class TopicRecreatedError(Errors.KafkaError):
    """A topic name now refers to a different topic identity."""


class BoundedFetchSession:
    """Read explicit partitions only up to offsets captured at startup.

    The session is independent of consumer group assignment. Appends that
    arrive after construction do not extend the read range. Completion is an
    offset comparison, so gaps caused by compaction, deletion, and aborted
    transactions do not make the session wait for a fixed record count.
    """

    def __init__(self, client, partitions, start_offsets,
                 timeout_ms=None, max_records=None, **configs):
        self._client = client
        self._manager = client._manager
        self._net = client._manager._net
        self._cluster = client._manager.cluster
        self._partitions = list(partitions)
        self._start_offsets = dict(start_offsets)
        self._timeout_ms = configs.get('default_api_timeout_ms', 60000) if timeout_ms is None else timeout_ms
        self._max_records = configs.get('max_poll_records') if max_records is None else max_records
        self._boundaries = {}
        self._topic_ids = {}
        self._closed = False
        self._start_time = time.monotonic()

        self._validate_inputs()

        configs = dict(configs)
        configs.update({
            'fetch_min_bytes': 0,
            'fetch_max_wait_ms': 0,
            'enable_incremental_fetch_sessions': False,
            'metrics': None,
            'metric_group_prefix': 'bounded_consumer',
        })
        self._subscriptions = SubscriptionState(None)
        self._subscriptions.assign_from_user(self._partitions)
        self._fetcher = Fetcher(client, self._subscriptions, **configs)

        self._net.run(self._capture_bounds, timeout_ms=self._timeout_ms)

    def _validate_inputs(self):
        if not self._partitions:
            raise ValueError('partitions must not be empty')
        if len(set(self._partitions)) != len(self._partitions):
            raise ValueError('partitions must not contain duplicates')
        if not all(isinstance(tp, TopicPartition) for tp in self._partitions):
            raise TypeError('partitions must contain TopicPartition values')
        if not all(isinstance(offset, int) for offset in self._start_offsets.values()):
            raise TypeError('start_offsets must map partitions to integer offsets')
        if any(offset < 0 for offset in self._start_offsets.values()):
            raise ValueError('start offsets must not be negative')
        missing = set(self._partitions).difference(self._start_offsets)
        if missing:
            raise ValueError('missing start offsets: %s' % (sorted(missing),))
        extra = set(self._start_offsets).difference(self._partitions)
        if extra:
            raise ValueError('unexpected start offsets: %s' % (sorted(extra),))
        if self._max_records is not None and (not isinstance(self._max_records, int) or self._max_records <= 0):
            raise ValueError('max_records must be a positive integer')

    async def _capture_bounds(self):
        topics = sorted({tp.topic for tp in self._partitions})
        self._cluster.set_topics(topics)
        await self._refresh_metadata()
        self._capture_identities()

        for tp in self._partitions:
            self._subscriptions.seek(tp, self._start_offsets[tp])

        timestamps = {tp: OffsetSpec.LATEST for tp in self._partitions}
        offsets = await self._fetcher._fetch_offsets_by_times_async(
            timestamps, self._timeout_ms)
        self._verify_identities()

        for tp in self._partitions:
            if tp not in offsets:
                raise Errors.KafkaError('Missing end offset for %s' % (tp,))
            boundary = offsets[tp].offset
            if boundary < self._start_offsets[tp]:
                raise Errors.OffsetOutOfRangeError({tp: (self._start_offsets[tp], boundary)})
            self._boundaries[tp] = boundary

    async def _refresh_metadata(self):
        deadline = time.monotonic() + self._timeout_ms / 1000
        while True:
            self._cluster.request_update()
            try:
                future = self._cluster._refresh_future or self._cluster.request_update()
                await self._net.await_for(
                    future,
                    timeout_ms=max(0, int((deadline - time.monotonic()) * 1000)))
                return
            except Errors.RetriableError:
                if time.monotonic() >= deadline:
                    raise
                await self._net.sleep(min(
                    self._fetcher.config['retry_backoff_ms'] / 1000,
                    max(0, deadline - time.monotonic())))

    def _capture_identities(self):
        for tp in self._partitions:
            leader = self._cluster.leader_for_partition(tp)
            if leader is None or leader == -1:
                raise Errors.LeaderNotAvailableError(str(tp))
            topic_id = self._cluster.topic_id(tp.topic)
            if topic_id is None:
                raise Errors.UnsupportedVersionError(
                    'Bounded fetch sessions require Metadata API v10 topic IDs')
            self._topic_ids[tp.topic] = topic_id

    def _verify_identities(self):
        for tp in self._partitions:
            current_id = self._cluster.topic_id(tp.topic)
            if current_id is None:
                raise Errors.UnknownTopicOrPartitionError(str(tp))
            if current_id != self._topic_ids[tp.topic]:
                raise TopicRecreatedError(
                    '%s changed identity from %s to %s' % (
                        tp.topic, self._topic_ids[tp.topic], current_id))
            if self._cluster.leader_for_partition(tp) is None:
                raise Errors.UnknownTopicOrPartitionError(str(tp))

    def poll(self, timeout_ms=None):
        """Fetch the next records within the captured offset ranges."""
        if self._closed:
            raise Errors.IllegalStateError('Bounded fetch session is closed')
        if timeout_ms is not None and timeout_ms < 0:
            raise ValueError('timeout_ms must not be negative')
        if self.finished():
            return {}

        records = self._net.run(
            self._poll_async, timeout_ms if timeout_ms is not None else 0,
            timeout_ms=self._remaining_ms())
        return self._trim_to_bounds(records)

    async def _poll_async(self, timeout_ms):
        self._verify_identities()
        self._seek_to_bounds()
        requests = self._fetcher._create_fetch_requests()
        needs_metadata = False
        if not requests:
            await self._refresh_metadata()
            self._verify_identities()
            requests = self._fetcher._create_fetch_requests()

        for node_id, (request, fetch_offsets) in requests.items():
            response = await self._manager.send(request, node_id=node_id)
            needs_metadata = needs_metadata or any(
                Errors.for_code(partition_data.error_code) in (
                    Errors.NotLeaderForPartitionError,
                    Errors.ReplicaNotAvailableError,
                    Errors.UnknownTopicOrPartitionError,
                    Errors.KafkaStorageError,
                )
                for topic_data in response.responses
                for partition_data in topic_data.partitions)
            if needs_metadata:
                continue
            self._fetcher._handle_fetch_response(
                node_id, fetch_offsets, time.monotonic(), response)

        records, _ = self._fetcher.fetched_records(
            max_records=self._max_records, update_offsets=True)
        if needs_metadata:
            await self._refresh_metadata()
            self._verify_identities()
        self._seek_to_bounds()
        return records

    def _seek_to_bounds(self):
        for tp in self._partitions:
            position = self._subscriptions.position(tp)
            boundary = self._boundaries[tp]
            if position.offset > boundary:
                self._subscriptions.seek(tp, boundary)

    def _trim_to_bounds(self, records):
        trimmed = {}
        for tp in list(records):
            visible = [record for record in records[tp] if record.offset < self._boundaries[tp]]
            if visible:
                trimmed[tp] = visible
        self._seek_to_bounds()
        return trimmed

    def finished(self):
        """Return True when every partition progress has reached its boundary."""
        return bool(self._boundaries) and all(
            self._subscriptions.position(tp).offset >= self._boundaries[tp]
            for tp in self._partitions)

    def progress(self):
        """Return the current next fetch offset for each partition."""
        return {tp: self._subscriptions.position(tp).offset for tp in self._partitions}

    def boundaries(self):
        """Return the exclusive end offset captured for each partition."""
        return dict(self._boundaries)

    def result(self):
        """Return progress and boundary for each partition after completion."""
        if not self.finished():
            raise Errors.IllegalStateError('Bounded fetch session is not complete')
        return {tp: BoundedPartitionResult(
            self._subscriptions.position(tp).offset, self._boundaries[tp])
            for tp in self._partitions}

    def close(self):
        if not self._closed:
            self._fetcher.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _remaining_ms(self):
        elapsed_ms = int((time.monotonic() - self._start_time) * 1000)
        return max(0, self._timeout_ms - elapsed_ms)
