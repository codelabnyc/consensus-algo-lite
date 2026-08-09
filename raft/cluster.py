"""The test harness.

It builds the cluster (the nodes, one shared clock, one shared network) and
moves the whole system forward one step at a time. Each step does three things
in order:
    1. move time forward
    2. deliver any messages that have arrived
    3. let every node react (check timers, send heartbeats, hold elections)

Tests use run_until(...) to step until something is true -- e.g. "a leader is
elected" -- then inspect who is leader and what got committed.
"""

import random

from .clock import Clock
from .network import Network
from .node import Node, LEADER
from .kv import KV


class Cluster:
    def __init__(self, n, seed=1, latency=10):
        self.clock = Clock()
        self.network = Network(self.clock, latency=latency)
        self.nodes = {}
        ids = list(range(n))
        for i in ids:
            peers = [j for j in ids if j != i]
            node = Node(
                node_id=i,
                peers=peers,
                network=self.network,
                clock=self.clock,
                rng=random.Random(seed * 100 + i),  # deterministic per node
                state_machine=KV(),
            )
            self.nodes[i] = node
            self.network.register(node)

    def step(self, dt=10):
        self.clock.advance(dt)              # time passes
        self.network.deliver_due()          # Deliver every message whose time has arrived.
        for node in self.nodes.values():    # every node reacts
            node.tick()

    def run(self, duration, dt=10):
        end = self.clock.now() + duration
        while self.clock.now() < end:       # keep stepping until enough time passed
            self.step(dt)

    def run_until(self, predicate, max_steps=2000, dt=10):
        for _ in range(max_steps):
            self.step(dt)
            if predicate(self):
                return True
        return False

    # --- inspection helpers ---------------------------------------------
    def live_nodes(self):
        """Nodes that are up AND not cut off."""
        return [n for n in self.nodes.values()
                if n.up and n.id not in self.network.isolated]

    def leaders(self):
        """Which live nodes think they're leader."""
        return [n for n in self.live_nodes() if n.role == LEADER]

    def leader(self):
        """Grab the one leader, or None."""
        ls = self.leaders()
        return ls[0] if ls else None
