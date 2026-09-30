"""Capacity-aware (weighted) partition assignor.

An opt-in alternative to the count-based assignors (range, roundrobin,
sticky). Each member declares a positive integer 'capacity' in its
subscription metadata; the leader then targets a partition load
proportional to each member's capacity instead of a uniform share.

Assignment contract (lexicographic priority):

  1. Capacity balance - minimize the sum of squared deviations between
     each member's assigned load and its capacity-proportional share of
     the total partition count. Deviations are computed in scaled
     integer units: for member i with load L_i and capacity c_i, total
     capacity C and total partitions P, the deviation is
     L_i * C - P * c_i.
  2. Movement - among assignments with equal balance, minimize the
     number of partitions whose owner differs from the previous
     generation (as advertised via owned_partitions).
  3. Canonical tie-break - any remaining ties are broken by the
     lexicographically smallest canonical assignment (members and
     partitions sorted), so the result never depends on the order in
     which member ids are iterated.

Subscriptions are always respected: a partition is only ever assigned
to a member subscribed to its topic, even when capacity demands more.

The assignor is cooperative-only (KIP-429): partitions that move
between owners are withheld from the new owner's first-round
assignment, so the old owner always revokes first and no partition is
ever assigned to two members in the same generation.

Members that do not declare a capacity (empty or undecodable user
data) are assigned DEFAULT_CAPACITY. Other assignors are unaffected -
this assignor only activates when explicitly configured via
partition_assignment_strategy.
"""

import logging
from collections import defaultdict

from kafka.coordinator.assignors.abstract import (
    AbstractPartitionAssignor,
    RebalanceProtocol,
)
from kafka.protocol.consumer.metadata import (
    ConsumerProtocolAssignment,
    ConsumerProtocolSubscription,
)
from kafka.protocol.old.struct import Struct
from kafka.protocol.old.types import Int32, Schema
from kafka.structs import TopicPartition

log = logging.getLogger(__name__)


class CapacityAwareAssignorUserData(Struct):
    """User-data payload carrying a member's declared capacity."""

    SCHEMA = Schema(
        ('capacity', Int32),
    )


class CapacityAwarePartitionAssignor(AbstractPartitionAssignor):
    """Assign partitions proportional to per-member declared capacity.

    See the module docstring for the full assignment contract.
    """

    DEFAULT_CAPACITY = 1
    DEFAULT_GENERATION_ID = -1

    name = 'capacity-aware'
    # ConsumerProtocolSubscription v1 carries owned_partitions, which
    # the leader needs both for movement minimization and for the
    # cooperative two-phase handoff.
    version = 1

    # Above this many legal assignments the exact solver falls back to
    # a deterministic greedy + local-search heuristic. Small groups
    # (the common case, and all conformance tests) are always solved
    # by exhaustive enumeration of every legal assignment.
    _MAX_ENUMERATED_ASSIGNMENTS = 200000
    _MAX_EXHAUSTIVE_DEPTH = 100

    def __init__(self, capacity=DEFAULT_CAPACITY):
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError(
                'capacity must be a positive integer, got %r' % (capacity,))
        self._capacity = capacity
        self.member_assignment = None
        self.generation = self.DEFAULT_GENERATION_ID

    def supported_protocols(self):
        # COOPERATIVE only - moved partitions must be released by the
        # old owner before the new owner may pick them up, which
        # requires the incremental protocol.
        return [RebalanceProtocol.COOPERATIVE]

    def metadata(self, topics):
        SubTP = ConsumerProtocolSubscription.TopicPartition
        owned_partitions = []
        if self.member_assignment is not None:
            by_topic = defaultdict(list)
            for tp in self.member_assignment:
                by_topic[tp.topic].append(tp.partition)
            owned_partitions = [
                SubTP(topic=topic, partitions=sorted(partitions))
                for topic, partitions in sorted(by_topic.items())
            ]
        return ConsumerProtocolSubscription(
            version=self.version,
            topics=sorted(topics),
            user_data=CapacityAwareAssignorUserData(capacity=self._capacity).encode(),
            owned_partitions=owned_partitions,
        )

    def on_assignment(self, assignment, generation):
        self.member_assignment = assignment.partitions()
        self.generation = generation

    @classmethod
    def parse_capacity(cls, user_data):
        """Extract a member's declared capacity from subscription user_data.

        Missing, undecodable, or non-positive declarations fall back to
        DEFAULT_CAPACITY so a misbehaving member cannot break the
        leader's assignment.
        """
        if not user_data:
            return cls.DEFAULT_CAPACITY
        if isinstance(user_data, CapacityAwareAssignorUserData):
            capacity = user_data.capacity
        else:
            try:
                capacity = CapacityAwareAssignorUserData.decode(user_data).capacity
            except Exception:
                log.warning('Could not parse capacity from member user_data;'
                            ' using default capacity %d', cls.DEFAULT_CAPACITY)
                return cls.DEFAULT_CAPACITY
        if not isinstance(capacity, int) or capacity < 1:
            log.warning('Invalid declared capacity %r; using default capacity %d',
                        capacity, cls.DEFAULT_CAPACITY)
            return cls.DEFAULT_CAPACITY
        return capacity

    def assign(self, cluster, members):
        """Compute the cooperative assignment for the group.

        The ideal final assignment is solved with the contract
        described in the module docstring; partitions that are moving
        to a different owner are then withheld from the new owner's
        first-round assignment so the old owner revokes first.
        """
        member_ids = sorted(member.member_id for member in members)
        subscriptions = {}
        capacities = {}
        # partition -> set of current-group member ids claiming ownership
        owned = {}
        for member in members:
            metadata = member.metadata
            subscriptions[member.member_id] = set(metadata.topics)
            capacities[member.member_id] = self.parse_capacity(
                getattr(metadata, 'user_data', None))
            for tp_data in getattr(metadata, 'owned_partitions', None) or ():
                for partition in tp_data.partitions:
                    tp = TopicPartition(tp_data.topic, partition)
                    owned.setdefault(tp, set()).add(member.member_id)

        all_topics = set()
        for topics in subscriptions.values():
            all_topics.update(topics)

        # partition -> sorted list of eligible (subscribed) member ids
        eligible = {}
        for topic in sorted(all_topics):
            partitions = cluster.partitions_for_topic(topic)
            if partitions is None:
                log.warning('No partition metadata for topic %s', topic)
                continue
            subscribers = [
                member_id for member_id in member_ids
                if topic in subscriptions[member_id]
            ]
            if not subscribers:
                continue
            for partition in sorted(partitions):
                eligible[TopicPartition(topic, partition)] = subscribers

        final = self._solve(member_ids, capacities, eligible, owned)

        # Cooperative round 1: a partition is only included for its new
        # owner if nobody else still owns it. Moving (or conflicting)
        # partitions are deferred; the old owner revokes them and a
        # follow-up rebalance lands them on the intended owner.
        protocol_assignment = {}
        for member in members:
            member_id = member.member_id
            kept_by_topic = defaultdict(list)
            for tp in final[member_id]:
                owners = owned.get(tp)
                if not owners or owners == {member_id}:
                    kept_by_topic[tp.topic].append(tp.partition)
            protocol_assignment[member_id] = ConsumerProtocolAssignment(
                self.version,
                sorted((topic, sorted(partitions))
                       for topic, partitions in kept_by_topic.items()),
                b'')
        return protocol_assignment

    @classmethod
    def _solve(cls, member_ids, capacities, eligible, owned):
        """Return {member_id: [TopicPartition, ...]} per the contract."""
        partitions = sorted(eligible)
        total_capacity = sum(capacities[m] for m in member_ids)
        num_partitions = len(partitions)

        def balance_score(loads):
            return sum(
                (loads[m] * total_capacity - num_partitions * capacities[m]) ** 2
                for m in member_ids
            )

        def move_delta(tp, member_id):
            owners = owned.get(tp)
            return 0 if not owners or member_id in owners else 1

        enumeration = 1
        for tp in partitions:
            enumeration *= len(eligible[tp])
            if enumeration > cls._MAX_ENUMERATED_ASSIGNMENTS:
                break
        if (enumeration <= cls._MAX_ENUMERATED_ASSIGNMENTS
                and len(partitions) <= cls._MAX_EXHAUSTIVE_DEPTH):
            return cls._solve_exhaustive(
                member_ids, partitions, eligible, owned,
                balance_score, move_delta)
        return cls._solve_greedy(
            member_ids, partitions, eligible, owned,
            balance_score, move_delta)

    @classmethod
    def _solve_exhaustive(cls, member_ids, partitions, eligible, owned,
                          balance_score, move_delta):
        """Enumerate every legal assignment and keep the best by
        (balance score, movement, canonical form)."""
        loads = {m: 0 for m in member_ids}
        current = {m: [] for m in member_ids}
        best = {'key': None, 'assignment': None}

        def canonical():
            return tuple((m, tuple(current[m])) for m in member_ids)

        def search(idx, moves):
            if idx == len(partitions):
                key = (balance_score(loads), moves, canonical())
                if best['key'] is None or key < best['key']:
                    best['key'] = key
                    best['assignment'] = {
                        m: list(tps) for m, tps in current.items()}
                return
            tp = partitions[idx]
            for member_id in eligible[tp]:
                loads[member_id] += 1
                current[member_id].append(tp)
                search(idx + 1, moves + move_delta(tp, member_id))
                current[member_id].pop()
                loads[member_id] -= 1

        search(0, 0)
        return best['assignment']

    @classmethod
    def _solve_greedy(cls, member_ids, partitions, eligible, owned,
                      balance_score, move_delta):
        """Deterministic fallback for large groups: greedy seeding in
        canonical partition order, then single-partition local moves
        until no (balance, movement) improvement exists."""
        loads = {m: 0 for m in member_ids}
        assignment = {m: [] for m in member_ids}
        owner = {}
        moves = 0
        for tp in partitions:
            best_member = None
            best_key = None
            for member_id in eligible[tp]:
                loads[member_id] += 1
                key = (balance_score(loads), move_delta(tp, member_id), member_id)
                loads[member_id] -= 1
                if best_key is None or key < best_key:
                    best_key = key
                    best_member = member_id
            loads[best_member] += 1
            assignment[best_member].append(tp)
            owner[tp] = best_member
            moves += move_delta(tp, best_member)

        score = balance_score(loads)
        while True:
            best_move = None
            for tp in partitions:
                current_owner = owner[tp]
                for member_id in eligible[tp]:
                    if member_id == current_owner:
                        continue
                    loads[current_owner] -= 1
                    loads[member_id] += 1
                    candidate = (
                        balance_score(loads),
                        moves - move_delta(tp, current_owner)
                              + move_delta(tp, member_id),
                    )
                    loads[current_owner] += 1
                    loads[member_id] -= 1
                    if candidate < (score, moves) and (
                            best_move is None or candidate < best_move[0]):
                        best_move = (candidate, tp, member_id)
            if best_move is None:
                break
            (score, moves), tp, new_owner = best_move
            old_owner = owner[tp]
            loads[old_owner] -= 1
            loads[new_owner] += 1
            assignment[old_owner].remove(tp)
            assignment[new_owner].append(tp)
            owner[tp] = new_owner
        return assignment
