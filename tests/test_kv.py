"""Step 5: the key-value state machine on top of the committed log."""

from raft.cluster import Cluster


def test_read_after_write_across_nodes():
    c = Cluster(5, seed=1)
    assert c.run_until(lambda cl: cl.leader() is not None)
    leader = c.leader()

    leader.client_submit({"op": "put", "key": "x", "value": 42})
    assert c.run_until(lambda cl: all(
        n.sm.get("x") == 42 for n in cl.live_nodes()))
    # Every live node's state machine reflects the write.
    for n in c.live_nodes():
        assert n.sm.get("x") == 42


def test_delete_replicates():
    c = Cluster(5, seed=2)
    assert c.run_until(lambda cl: cl.leader() is not None)
    leader = c.leader()

    leader.client_submit({"op": "put", "key": "y", "value": 1})
    assert c.run_until(lambda cl: all(
        n.sm.get("y") == 1 for n in cl.live_nodes()))

    leader.client_submit({"op": "delete", "key": "y"})
    assert c.run_until(lambda cl: all(
        n.sm.get("y") is None for n in cl.live_nodes()))
