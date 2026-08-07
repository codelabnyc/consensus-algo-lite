"""Step 2 smoke test: the simulated clock + network deliver a message."""

from raft.clock import Clock
from raft.network import Network


class Recorder:
    """Minimal stand-in for a Node: records what it receives."""
    def __init__(self, node_id):
        self.id = node_id
        self.up = True
        self.inbox = []

    def receive(self, src, msg):
        self.inbox.append((src, msg))


def test_message_delivered_after_latency():
    clock = Clock()
    net = Network(clock, latency=10)
    a, b = Recorder(0), Recorder(1)
    net.register(a)
    net.register(b)

    net.send(0, 1, "hello")
    net.deliver_due()          # too early: latency not elapsed
    assert b.inbox == []

    clock.advance(10)
    net.deliver_due()
    assert b.inbox == [(0, "hello")]


def test_isolated_node_receives_nothing():
    clock = Clock()
    net = Network(clock, latency=10)
    a, b = Recorder(0), Recorder(1)
    net.register(a)
    net.register(b)

    net.isolate(1)
    net.send(0, 1, "hello")
    clock.advance(10)
    net.deliver_due()
    assert b.inbox == []
