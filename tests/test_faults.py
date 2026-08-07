"""Step 6: fault injection and safety under partitions."""

from raft.cluster import Cluster
from raft.node import LEADER
from .invariants import (
    one_leader_per_term, record_leaders, committed_prefixes_agree,
)


def test_partition_no_split_brain():
    c = Cluster(5, seed=1)
    assert c.run_until(lambda cl: cl.leader() is not None)

    # Split into a minority {0,1} and a majority {2,3,4}.
    c.network.partition([0, 1])
    observed = {}
    for _ in range(300):
        c.step()
        record_leaders(c, observed)

    # Safety: never two leaders in one term, even split across partitions.
    assert one_leader_per_term(observed)
    # Only the majority side can hold a functioning leader.
    majority_leaders = [n for n in (c.nodes[2], c.nodes[3], c.nodes[4])
                        if n.role == LEADER]
    assert len(majority_leaders) == 1


def test_majority_stays_available_with_minority_down():
    c = Cluster(5, seed=2)
    assert c.run_until(lambda cl: cl.leader() is not None)

    # Kill a minority (2 of 5); a majority of 3 remains.
    c.network.isolate(0)
    c.network.isolate(1)
    assert c.run_until(lambda cl: cl.leader() is not None)
    leader = c.leader()

    idx = leader.client_submit({"op": "put", "key": "a", "value": 9})
    assert c.run_until(lambda cl: leader.commit_index >= idx)  # still commits
    assert committed_prefixes_agree(c.live_nodes())


def test_divergent_follower_log_is_repaired():
    c = Cluster(5, seed=3)
    assert c.run_until(lambda cl: cl.leader() is not None)
    old = c.leader()

    # Isolate the leader with one follower; writes here can't reach a majority,
    # so they never commit -- an uncommitted divergent tail.
    c.network.partition([old.id])
    for i in range(3):
        old.client_submit({"op": "put", "key": "z", "value": i})
    c.run(200)
    assert old.commit_index == 0  # nothing committed on the minority side

    # The majority elects a new leader and commits its own entry.
    assert c.run_until(
        lambda cl: any(n.role == LEADER and n.id != old.id
                       for n in cl.live_nodes()))
    new = next(n for n in c.live_nodes() if n.role == LEADER)
    new.client_submit({"op": "put", "key": "z", "value": 99})
    assert c.run_until(lambda cl: new.commit_index >= 1)

    # Heal: the old leader must discard its divergent tail and converge.
    c.network.heal()
    assert c.run_until(
        lambda cl: old.log[1:new.commit_index + 1] == new.log[1:new.commit_index + 1],
        max_steps=3000)
    assert committed_prefixes_agree(list(c.nodes.values()))
