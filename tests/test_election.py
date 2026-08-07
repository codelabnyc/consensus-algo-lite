"""Step 3: leader election."""

from raft.cluster import Cluster
from raft.node import FOLLOWER, LEADER
from .invariants import one_leader_per_term, record_leaders


def test_elects_exactly_one_leader():
    c = Cluster(5, seed=1)
    assert c.run_until(lambda cl: cl.leader() is not None)
    assert len(c.leaders()) == 1


def test_at_most_one_leader_per_term():
    c = Cluster(5, seed=2)
    observed = {}
    for _ in range(200):
        c.step()
        record_leaders(c, observed)
    assert one_leader_per_term(observed)


def test_reelection_after_leader_isolated():
    c = Cluster(5, seed=3)
    assert c.run_until(lambda cl: cl.leader() is not None)
    old = c.leader()
    old_term = old.current_term

    c.network.isolate(old.id)  # leader "crashes"
    assert c.run_until(
        lambda cl: cl.leader() is not None and cl.leader().id != old.id)
    new = c.leader()
    assert new.id != old.id
    assert new.current_term > old_term  # elected in a later term


def test_isolated_leader_steps_down_on_rejoin():
    c = Cluster(5, seed=4)
    assert c.run_until(lambda cl: cl.leader() is not None)
    old = c.leader()

    c.network.isolate(old.id)
    assert c.run_until(
        lambda cl: cl.leader() is not None and cl.leader().id != old.id)

    c.network.rejoin(old.id)  # stale leader comes back
    c.run(500)
    # It must discover the newer term and revert; never two leaders at once.
    assert old.role == FOLLOWER
    assert len(c.leaders()) == 1
