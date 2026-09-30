from fractions import Fraction
from itertools import product

import pytest

from kafka.coordinator.assignors.abstract import RebalanceProtocol
from kafka.coordinator.assignors.capacity import (
    CapacityPartitionAssignor,
    CooperativeCapacityPartitionAssignor,
)
from kafka.protocol.consumer import JoinGroupResponse
from kafka.protocol.consumer.metadata import ConsumerProtocolSubscription
from kafka.structs import TopicPartition
import kafka.errors as Errors

from test.consumer.test_assignors import create_cluster


def member(member_id, subscription, group_instance_id=None):
    return JoinGroupResponse.JoinGroupResponseMember(
        member_id=member_id,
        group_instance_id=group_instance_id,
        metadata=subscription,
    )


def members_from_metadata(group_subscriptions):
    return [
        member(member_id, subscription)
        for member_id, subscription in group_subscriptions.items()
    ]


def partitions_for(topics):
    return [
        TopicPartition(topic, partition)
        for topic, partition_count in topics.items()
        for partition in range(partition_count)
    ]


def legal_assignments(partitions, subscriptions, member_ids):
    subscribers = [
        tuple(member_id for member_id in member_ids if partition.topic in subscriptions[member_id])
        for partition in partitions
    ]
    for owners in product(*subscribers):
        assignment = {member_id: set() for member_id in member_ids}
        for partition, owner in zip(partitions, owners):
            assignment[owner].add(partition)
        yield assignment


def balance_key(assignment, capacities):
    return sum(
        Fraction(len(partitions) ** 2, capacities[member_id])
        for member_id, partitions in assignment.items()
    )


def movement_key(assignment, previous):
    return sum(
        partition not in previous.get(member_id, set())
        for member_id, partitions in assignment.items()
        for partition in partitions
    )


def canonical_key(assignment, member_ids, partitions):
    ordered = []
    for partition in sorted(partitions):
        for rank, member_id in enumerate(sorted(member_ids)):
            if partition in assignment[member_id]:
                ordered.append(rank)
    return tuple(ordered)


def expected_assignment(partitions, subscriptions, capacities, previous, member_ids):
    best = min(
        legal_assignments(partitions, subscriptions, member_ids),
        key=lambda assignment: (
            balance_key(assignment, capacities),
            movement_key(assignment, previous),
            canonical_key(assignment, member_ids, partitions),
        ),
    )
    return {member_id: frozenset(best[member_id]) for member_id in member_ids}


def to_sets(assignments):
    return {member_id: set(assignment.partitions()) for member_id, assignment in assignments.items()}


def assert_matches_exhaustive(result, partitions, subscriptions, capacities, previous):
    member_ids = sorted(subscriptions)
    actual = to_sets(result)
    expected = expected_assignment(partitions, subscriptions, capacities, previous, member_ids)
    assert {member_id: frozenset(partitions) for member_id, partitions in actual.items()} == expected


@pytest.mark.parametrize('member_order', [0, 1])
def test_capacity_assignment_matches_exhaustive_optimum(mocker, member_order):
    topics = {'t0': 2, 't1': 3}
    capacities = {'C0': 1, 'C1': 2, 'C2': 3}
    subscriptions = {
        'C0': {'t0'},
        'C1': {'t0', 't1'},
        'C2': {'t0', 't1'},
    }
    items = list(subscriptions.items())
    if member_order:
        items.reverse()
    members = [
        member(member_id, CapacityPartitionAssignor(capacities[member_id]).metadata(topics_))
        for member_id, topics_ in items
    ]
    cluster = create_cluster(mocker, set(topics), topics_partitions=None,
                             topic_partitions_lambda=lambda topic: set(range(topics[topic])))

    result = CapacityPartitionAssignor().assign(cluster, members)

    assert_matches_exhaustive(
        result,
        partitions_for(topics),
        subscriptions,
        capacities,
        {member_id: set() for member_id in subscriptions},
    )


def test_capacity_defaults_to_one_and_round_trips_metadata():
    assignor = CapacityPartitionAssignor()
    metadata = assignor.metadata({'t'})
    encoded = ConsumerProtocolSubscription.decode(metadata.encode())
    assert encoded.user_data is not None
    assert assignor.parse_member_metadata(encoded) == (1, [], -1)


@pytest.mark.parametrize('capacity', [0, -1, 1.5, True, '2', 2 ** 31])
def test_capacity_must_be_positive_integer(capacity):
    with pytest.raises(Errors.KafkaConfigurationError):
        CapacityPartitionAssignor(capacity)


def test_only_assigns_subscribed_partitions(mocker):
    cluster = create_cluster(mocker, {'a', 'b'}, topics_partitions={0, 1, 2})
    members = members_from_metadata({
        'A': CapacityPartitionAssignor(2).metadata({'a'}),
        'B': CapacityPartitionAssignor(1).metadata({'b'}),
    })

    result = CapacityPartitionAssignor().assign(cluster, members)

    assert result['A'].partitions() == [
        TopicPartition('a', 0), TopicPartition('a', 1), TopicPartition('a', 2),
    ]
    assert result['B'].partitions() == [
        TopicPartition('b', 0), TopicPartition('b', 1), TopicPartition('b', 2),
    ]


def test_movement_is_minimized_only_after_balance(mocker):
    assignor = CapacityPartitionAssignor()
    cluster = create_cluster(mocker, {'t'}, topics_partitions=set(range(8)))
    previous = {
        'small': {TopicPartition('t', number) for number in range(2)},
        'big': {TopicPartition('t', number) for number in range(2, 8)},
    }
    members = [
        member('small', assignor._metadata({'t'}, previous['small'])),
        member('big', CapacityPartitionAssignor(3)._metadata({'t'}, previous['big'])),
    ]

    result = assignor.assign(cluster, members)
    assigned = to_sets(result)

    assert len(assigned['small']) == 2
    assert len(assigned['big']) == 6
    assert assigned['small'] == previous['small']
    assert assigned['big'] == previous['big']


def test_dynamic_member_removal_reassigns_only_needed_partitions(mocker):
    cluster = create_cluster(mocker, {'t'}, topics_partitions=set(range(6)))
    first = members_from_metadata({
        'old': CapacityPartitionAssignor(1).metadata({'t'}),
        'stay': CapacityPartitionAssignor(1).metadata({'t'}),
    })
    first_result = CapacityPartitionAssignor().assign(cluster, first)
    stay_partitions = set(first_result['stay'].partitions())

    second = members_from_metadata({
        'stay': CapacityPartitionAssignor(1)._metadata({'t'}, stay_partitions),
    })
    second_result = CapacityPartitionAssignor().assign(cluster, second)

    assert set(second_result['stay'].partitions()) >= stay_partitions
    assert len(second_result['stay'].partitions()) == 6


def test_static_identity_moves_without_dynamic_member_id(mocker):
    cluster = create_cluster(mocker, {'t'}, topics_partitions={0, 1, 2})
    old_member = member(
        'dynamic-old',
        CapacityPartitionAssignor(2)._metadata({'t'}, [TopicPartition('t', 0), TopicPartition('t', 1)]),
        group_instance_id='static',
    )
    new_member = member(
        'dynamic-new',
        CapacityPartitionAssignor(2)._metadata(
            {'t'},
            [TopicPartition('t', 0), TopicPartition('t', 1)],
        ),
        group_instance_id='static',
    )
    peer = member('peer', CapacityPartitionAssignor(1).metadata({'t'}))

    result = CapacityPartitionAssignor().assign(cluster, [new_member, peer])
    assigned = to_sets(result)

    assert assigned['dynamic-new'] == {TopicPartition('t', 0), TopicPartition('t', 1)}
    assert assigned['peer'] == {TopicPartition('t', 2)}
    assert old_member.member_id != new_member.member_id


def test_stale_generation_conflict_prefers_newer_claim(mocker):
    cluster = create_cluster(mocker, {'t'}, topics_partitions={0, 1})
    stale_metadata = CapacityPartitionAssignor(1)._metadata({'t'}, [TopicPartition('t', 0)])
    current_metadata = CapacityPartitionAssignor(1)._metadata({'t'}, [TopicPartition('t', 0)])
    stale = ConsumerProtocolSubscription(
        version=2,
        topics=['t'],
        user_data=stale_metadata.user_data,
        owned_partitions=[ConsumerProtocolSubscription.TopicPartition(topic='t', partitions=[0])],
        generation_id=1,
    )
    current = ConsumerProtocolSubscription(
        version=2,
        topics=['t'],
        user_data=current_metadata.user_data,
        owned_partitions=[ConsumerProtocolSubscription.TopicPartition(topic='t', partitions=[0])],
        generation_id=2,
    )
    members = [
        member('stale', stale, group_instance_id='static-a'),
        member('current', current, group_instance_id='static-b'),
    ]

    result = CooperativeCapacityPartitionAssignor(1).assign(cluster, members)

    assert result['current'].partitions() == [TopicPartition('t', 0)]
    assert result['stale'].partitions() == [TopicPartition('t', 1)]


class TestCooperativeCapacityAssignor:
    def test_supported_protocol(self):
        assert CooperativeCapacityPartitionAssignor().supported_protocols() == [RebalanceProtocol.COOPERATIVE]

    def test_metadata_carries_owned_partitions_and_generation(self):
        assignor = CooperativeCapacityPartitionAssignor(3)
        assignor.member_assignment = [TopicPartition('t', 2), TopicPartition('t', 0)]
        assignor.generation = 7
        metadata = assignor.metadata({'t'})

        assert metadata.version == 2
        assert metadata.generation_id == 7
        assert metadata.owned_partitions == [
            ConsumerProtocolSubscription.TopicPartition(topic='t', partitions=[0, 2])
        ]
        decoded = ConsumerProtocolSubscription.decode(metadata.encode())
        assert decoded.generation_id == 7
        assert decoded.owned_partitions[0].partitions == [0, 2]

    def test_moving_partition_is_not_sent_to_two_members_in_round_one(self, mocker):
        cluster = create_cluster(mocker, {'t'}, topics_partitions=set(range(4)))
        sub_topic = ConsumerProtocolSubscription.TopicPartition

        def subscription(capacity, owned, generation):
            capacity_assignor = CooperativeCapacityPartitionAssignor(capacity)
            capacity_assignor.member_assignment = owned
            capacity_assignor.generation = generation
            metadata = capacity_assignor.metadata({'t'})
            return metadata

        members = [
            member('small', subscription(1, [TopicPartition('t', number) for number in range(3)], 4)),
            member('big', subscription(3, [], 4)),
        ]
        result = CooperativeCapacityPartitionAssignor().assign(cluster, members)
        assigned = to_sets(result)

        assert not (assigned['small'] & assigned['big'])
        moved_to_big = {TopicPartition('t', number) for number in range(3)} - assigned['small']
        assert moved_to_big
        assert not (moved_to_big & assigned['big'])
        assert assigned['small'] | assigned['big'] < set(partitions_for({'t': 4}))

    def test_round_two_assigns_released_partition(self, mocker):
        cluster = create_cluster(mocker, {'t'}, topics_partitions={0, 1})
        sub_topic = ConsumerProtocolSubscription.TopicPartition

        def subscription(capacity, owned, generation):
            metadata = CapacityPartitionAssignor(capacity)._metadata({'t'}, owned)
            return ConsumerProtocolSubscription(
                version=2,
                topics=['t'],
                user_data=metadata.user_data,
                owned_partitions=[sub_topic(topic='t', partitions=sorted(partition.partition for partition in owned))],
                generation_id=generation,
            )

        first_round = CooperativeCapacityPartitionAssignor().assign(cluster, [
            member('a', subscription(1, [TopicPartition('t', 0), TopicPartition('t', 1)], 1)),
            member('b', subscription(3, [], 1)),
        ])
        retained = set(first_round['a'].partitions())
        second_round = CooperativeCapacityPartitionAssignor().assign(cluster, [
            member('a', subscription(1, retained, 2)),
            member('b', subscription(3, [], 2)),
        ])
        assigned = to_sets(second_round)

        assert set(assigned['a']) | set(assigned['b']) == {TopicPartition('t', 0), TopicPartition('t', 1)}
        assert len(assigned['b']) == 1
