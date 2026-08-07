"""Step 4: log replication and commit."""

from raft.cluster import Cluster
from .invariants import committed_prefixes_agree


def elect(seed):
    c = Cluster(5, seed=seed)
    assert c.run_until(lambda cl: cl.leader() is not None)
    return c, c.leader()


def test_command_replicates_and_commits():
    c, leader = elect(seed=1)
    idx = leader.client_submit({"op": "put", "key": "x", "value": 1})
    assert idx is not None

    assert c.run_until(lambda cl: leader.commit_index >= idx)
    # A majority stored it, and every live node agrees on the committed prefix.
    stored = sum(1 for n in c.live_nodes() if n.last_log_index() >= idx)
    assert stored >= leader.majority()
    assert committed_prefixes_agree(c.live_nodes())


def test_multiple_commands_ordered():
    c, leader = elect(seed=2)
    for i in range(5):
        leader.client_submit({"op": "put", "key": "k", "value": i})
    assert c.run_until(lambda cl: leader.commit_index >= 5)
    # Log order is the submit order.
    cmds = [e.command["value"] for e in leader.log[1:6]]
    assert cmds == [0, 1, 2, 3, 4]


def test_lagging_follower_catches_up():
    c, leader = elect(seed=3)
    follower = next(n for n in c.nodes.values() if n.id != leader.id)

    c.network.isolate(follower.id)          # follower misses these writes
    for i in range(4):
        leader.client_submit({"op": "put", "key": "k", "value": i})
    assert c.run_until(lambda cl: leader.commit_index >= 4)

    c.network.rejoin(follower.id)           # comes back, must catch up
    assert c.run_until(lambda cl: follower.last_log_index() >= 4)
    assert follower.log[1:5] == leader.log[1:5]
