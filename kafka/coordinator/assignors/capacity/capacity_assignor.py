"""Capacity-weighted partition assignment strategies."""

from collections import defaultdict, namedtuple
from fractions import Fraction
import heapq
import logging

from kafka.coordinator.assignors.abstract import (
    AbstractPartitionAssignor,
    RebalanceProtocol,
)
from kafka.coordinator.assignors.capacity.user_data import CapacityAssignorUserData
import kafka.errors as Errors
from kafka.protocol.consumer.metadata import (
    ConsumerProtocolAssignment,
    ConsumerProtocolSubscription,
)
from kafka.structs import TopicPartition

log = logging.getLogger(__name__)

DEFAULT_CAPACITY = 1
MAX_CAPACITY = 2 ** 31 - 1

MemberRecord = namedtuple(
    'MemberRecord',
    ('member_id', 'group_instance_id', 'identity', 'topics', 'capacity',
     'partitions', 'generation', 'sort_key'),
)


class _Cost(namedtuple('_Cost', ('balance', 'movement', 'ranks'))):
    __slots__ = ()

    @classmethod
    def zero(cls, rank_count):
        return cls(Fraction(0), 0, (0,) * rank_count)

    def __add__(self, other):
        return _Cost(
            self.balance + other.balance,
            self.movement + other.movement,
            tuple(left + right for left, right in zip(self.ranks, other.ranks)),
        )

    def __sub__(self, other):
        return _Cost(
            self.balance - other.balance,
            self.movement - other.movement,
            tuple(left - right for left, right in zip(self.ranks, other.ranks)),
        )

    def __neg__(self):
        return _Cost(
            -self.balance,
            -self.movement,
            tuple(-value for value in self.ranks),
        )


class _Edge:
    __slots__ = ('to', 'reverse', 'capacity', 'cost')

    def __init__(self, to, reverse, capacity, cost):
        self.to = to
        self.reverse = reverse
        self.capacity = capacity
        self.cost = cost


class _MinCostFlow:
    def __init__(self, node_count):
        self.graph = [[] for _ in range(node_count)]

    def add_edge(self, source, destination, capacity, cost):
        forward = _Edge(destination, len(self.graph[destination]), capacity, cost)
        backward = _Edge(source, len(self.graph[source]), 0, -cost)
        self.graph[source].append(forward)
        self.graph[destination].append(backward)
        return forward

    def flow(self, source, sink, required_flow):
        node_count = len(self.graph)
        rank_count = max(
            (len(edge.cost.ranks) for adjacency in self.graph for edge in adjacency),
            default=0,
        )
        potential = [_Cost.zero(rank_count)] * node_count
        total_flow = 0

        while total_flow < required_flow:
            distances = [None] * node_count
            previous_node = [-1] * node_count
            previous_edge = [-1] * node_count
            zero_cost = _Cost.zero(rank_count)
            distances[source] = zero_cost
            queue = [(zero_cost, source)]

            while queue:
                distance, node = heapq.heappop(queue)
                if distance != distances[node]:
                    continue
                for edge_index, edge in enumerate(self.graph[node]):
                    if edge.capacity <= 0:
                        continue
                    reduced_cost = distance + edge.cost - potential[node] + potential[edge.to]
                    if distances[edge.to] is None or reduced_cost < distances[edge.to]:
                        distances[edge.to] = reduced_cost
                        previous_node[edge.to] = node
                        previous_edge[edge.to] = edge_index
                        heapq.heappush(queue, (reduced_cost, edge.to))

            if distances[sink] is None:
                raise RuntimeError('Capacity assignment has no feasible solution')

            for node in range(node_count):
                if distances[node] is not None:
                    potential[node] = potential[node] + distances[node]

            augment_flow = required_flow - total_flow
            node = sink
            while node != source:
                edge = self.graph[previous_node[node]][previous_edge[node]]
                augment_flow = min(augment_flow, edge.capacity)
                node = previous_node[node]

            node = sink
            while node != source:
                predecessor = previous_node[node]
                edge_index = previous_edge[node]
                edge = self.graph[predecessor][edge_index]
                edge.capacity -= augment_flow
                self.graph[node][edge.reverse].capacity += augment_flow
                node = predecessor

            total_flow += augment_flow


class CapacityPartitionAssignor(AbstractPartitionAssignor):
    """Assign partitions in proportion to declared positive capacities."""

    name = 'capacity'
    version = 0
    subscription_version = 0
    DEFAULT_GENERATION_ID = -1

    def __init__(self, capacity=DEFAULT_CAPACITY):
        if (isinstance(capacity, bool) or not isinstance(capacity, int)
                or not 0 < capacity <= MAX_CAPACITY):
            raise Errors.KafkaConfigurationError('capacity must be a positive integer')
        self.capacity = capacity
        self.member_assignment = None
        self.generation = self.DEFAULT_GENERATION_ID

    def supported_protocols(self):
        return [RebalanceProtocol.EAGER]

    def _decode_user_data(self, metadata):
        user_data = metadata.user_data
        if not user_data:
            return None
        if not isinstance(user_data, CapacityAssignorUserData):
            try:
                user_data = CapacityAssignorUserData.decode(user_data)
            except Exception:
                log.exception('Could not parse capacity assignor member data')
                return None
        return user_data

    def parse_member_metadata(self, metadata):
        capacity = DEFAULT_CAPACITY
        partitions = []
        generation = self.DEFAULT_GENERATION_ID
        user_data = self._decode_user_data(metadata)

        if user_data is not None:
            declared_capacity = user_data.capacity
            if (isinstance(declared_capacity, int)
                    and 0 < declared_capacity <= MAX_CAPACITY):
                capacity = declared_capacity
            else:
                log.warning('Ignoring invalid member capacity %s', declared_capacity)
            for topic, topic_partitions in user_data.previous_assignment:
                partitions.extend(
                    TopicPartition(topic, partition)
                    for partition in topic_partitions
                )

        return capacity, partitions, generation

    def _parse_member(self, member):
        capacity, partitions, generation = self.parse_member_metadata(member.metadata)
        group_instance_id = member.group_instance_id
        identity = group_instance_id or member.member_id
        return MemberRecord(
            member_id=member.member_id,
            group_instance_id=group_instance_id,
            identity=identity,
            topics=frozenset(member.metadata.topics),
            capacity=capacity,
            partitions=partitions,
            generation=generation,
            sort_key=(
                group_instance_id is None,
                identity,
                member.member_id,
            ),
        )

    def assign(self, cluster, members):
        ordered_members = sorted(
            (self._parse_member(member) for member in members),
            key=lambda record: record.sort_key,
        )
        assignment = self._solve(cluster, ordered_members)
        return self._protocol_assignments(ordered_members, assignment)

    def _claim_owners(self, members, partitions):
        partition_set = set(partitions)
        claims = defaultdict(list)
        for record in members:
            for partition in record.partitions:
                if partition in partition_set:
                    claims[partition].append(record)

        owners = {}
        for partition, claimants in claims.items():
            winner = min(claimants, key=lambda record: (-record.generation, record.sort_key))
            if len({claimant.generation for claimant in claimants}) > 1:
                log.warning('Ignoring stale ownership claim for %s', partition)
            elif len(claimants) > 1:
                log.warning(
                    'Partition %s is claimed by multiple members in generation %s',
                    partition, winner.generation,
                )
            owners[partition] = winner.identity
        return owners

    def _solve(self, cluster, members):
        topics = sorted(set().union(*[record.topics for record in members])) if members else []
        partitions = []
        for topic in topics:
            topic_partitions = cluster.partitions_for_topic(topic)
            if topic_partitions is None:
                log.warning('No partition metadata for topic %s', topic)
                continue
            for partition in topic_partitions:
                partitions.append(TopicPartition(topic, partition))
        partitions.sort()

        owners = self._claim_owners(members, partitions)
        eligible = {}
        for partition in partitions:
            eligible[partition] = sorted(
                (record for record in members if partition.topic in record.topics),
                key=lambda record: record.sort_key,
            )

        assignment = {record.member_id: set() for record in members}
        if not partitions or not members:
            return assignment, owners

        source = 0
        partition_nodes = {partition: index + 1 for index, partition in enumerate(partitions)}
        member_nodes = {
            record.member_id: len(partitions) + index + 1
            for index, record in enumerate(members)
        }
        sink = len(partitions) + len(members) + 1
        network = _MinCostFlow(sink + 1)
        assignment_edges = {}
        zero_ranks = (0,) * len(partitions)

        for partition in partitions:
            partition_node = partition_nodes[partition]
            network.add_edge(
                source,
                partition_node,
                1,
                _Cost(Fraction(0), 0, zero_ranks),
            )
            assignment_edges[partition] = {}
            owner_identity = owners.get(partition)
            for rank, member in enumerate(eligible[partition]):
                movement_cost = 0 if member.identity == owner_identity else 1
                ranks = tuple(
                    rank if index == partition_node - 1 else 0
                    for index in range(len(partitions))
                )
                edge = network.add_edge(
                    partition_node,
                    member_nodes[member.member_id],
                    1,
                    _Cost(Fraction(0), movement_cost, ranks),
                )
                assignment_edges[partition][member.member_id] = edge

        for member in members:
            member_node = member_nodes[member.member_id]
            max_partitions = sum(
                len(cluster.partitions_for_topic(topic) or ())
                for topic in member.topics
            )
            for unit in range(max_partitions):
                balance_cost = Fraction(2 * unit + 1, member.capacity)
                network.add_edge(
                    member_node,
                    sink,
                    1,
                    _Cost(balance_cost, 0, zero_ranks),
                )

        network.flow(source, sink, len(partitions))

        for partition in partitions:
            for member_id, edge in assignment_edges[partition].items():
                if edge.capacity == 0:
                    assignment[member_id].add(partition)
                    break

        return assignment, owners

    def _protocol_assignments(self, members, assignment):
        assigned_partitions, _owners = assignment
        result = {}
        for member in members:
            by_topic = defaultdict(list)
            for partition in sorted(assigned_partitions[member.member_id]):
                by_topic[partition.topic].append(partition.partition)
            result[member.member_id] = ConsumerProtocolAssignment(
                self.version,
                sorted((topic, sorted(topic_partitions)) for topic, topic_partitions in by_topic.items()),
                b'',
            )
        return result

    def metadata(self, topics):
        return self._metadata(topics, self.member_assignment)

    def _metadata(self, topics, member_assignment_partitions):
        if member_assignment_partitions is None:
            previous_assignment = []
        else:
            by_topic = defaultdict(list)
            for partition in member_assignment_partitions:
                by_topic[partition.topic].append(partition.partition)
            previous_assignment = [
                (topic, sorted(topic_partitions))
                for topic, topic_partitions in sorted(by_topic.items())
            ]
        user_data = CapacityAssignorUserData(
            capacity=self.capacity,
            previous_assignment=previous_assignment,
        )
        return ConsumerProtocolSubscription(
            self.subscription_version,
            sorted(topics),
            user_data,
        )

    def on_assignment(self, assignment, generation):
        self.member_assignment = assignment.partitions()
        self.generation = generation


class CooperativeCapacityPartitionAssignor(CapacityPartitionAssignor):
    """Cooperative variant of the capacity-weighted assignor."""

    name = 'cooperative-capacity'
    subscription_version = 2

    def supported_protocols(self):
        return [RebalanceProtocol.COOPERATIVE]

    def parse_member_metadata(self, metadata):
        capacity, _partitions, _generation = super().parse_member_metadata(metadata)
        partitions = []
        for topic_partition in getattr(metadata, 'owned_partitions', None) or ():
            for partition in topic_partition.partitions:
                partitions.append(TopicPartition(topic_partition.topic, partition))
        return capacity, partitions, metadata.generation_id

    def assign(self, cluster, members):
        ordered_members = sorted(
            (self._parse_member(member) for member in members),
            key=lambda record: record.sort_key,
        )
        assigned_partitions, owners = self._solve(cluster, ordered_members)
        identities = {record.member_id: record.identity for record in ordered_members}
        staged = {
            member_id: {
                partition
                for partition in partitions
                if owners.get(partition) is None or owners.get(partition) == identities[member_id]
            }
            for member_id, partitions in assigned_partitions.items()
        }
        return self._protocol_assignments(ordered_members, (staged, owners))

    def metadata(self, topics):
        subscription = super().metadata(topics)
        subscription.generation_id = self.generation

        by_topic = defaultdict(list)
        if self.member_assignment is not None:
            for partition in self.member_assignment:
                by_topic[partition.topic].append(partition.partition)
        subscription.owned_partitions = [
            ConsumerProtocolSubscription.TopicPartition(
                topic=topic,
                partitions=sorted(topic_partitions),
            )
            for topic, topic_partitions in sorted(by_topic.items())
        ]
        return subscription
