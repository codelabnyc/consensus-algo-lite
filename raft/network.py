"""A controllable message bus: nodes send/receive through it, and tests can
drop messages, isolate nodes, or partition the cluster to prove the consensus
still stays safe.

Messages are queued with a delivery time (now + latency) and delivered when the
clock reaches it. No sockets, no real concurrency, fully deterministic.
"""

import heapq


class Network:
    def __init__(self, clock, latency=10):
        self.clock = clock
        self.latency = latency
        self.nodes = {}          # node_id -> Node
        self._queue = []         # heap of (deliver_at, seq, src, dst, msg)
        self._seq = 0
        self.isolated = set()    # node ids fully cut off from everyone
        self.blocked = set()     # frozenset({a, b}) links that are down

    def register(self, node):
        self.nodes[node.id] = node

    # --- fault injection -------------------------------------------------
    def isolate(self, node_id):
        """Cut one node off from the whole cluster (models a crash/partition)."""
        self.isolated.add(node_id)

    def rejoin(self, node_id):
        self.isolated.discard(node_id)

    def partition(self, group):
        """Sever every link between `group` and the rest of the cluster."""
        rest = [n for n in self.nodes if n not in group]
        for a in group:
            for b in rest:
                self.blocked.add(frozenset((a, b)))

    def heal(self):
        self.isolated.clear()
        self.blocked.clear()

    def _reachable(self, a, b):
        if a in self.isolated or b in self.isolated:
            return False
        return frozenset((a, b)) not in self.blocked

    # --- message passing -------------------------------------------------
    def send(self, src, dst, msg):
        if not self._reachable(src, dst):
            return  # dropped: link is down at send time
        heapq.heappush(
            self._queue,
            (self.clock.now() + self.latency, self._seq, src, dst, msg),
        )
        self._seq += 1

    def deliver_due(self):
        """Deliver every message whose time has arrived."""
        now = self.clock.now()
        while self._queue and self._queue[0][0] <= now:
            _, _, src, dst, msg = heapq.heappop(self._queue)
            if not self._reachable(src, dst):
                continue  # link went down after send -> message lost in flight
            node = self.nodes.get(dst)
            if node is not None and node.up:
                node.receive(src, msg)
