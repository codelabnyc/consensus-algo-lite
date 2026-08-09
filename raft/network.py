"""A controllable message bus: nodes send/receive through it, and tests can
drop messages, isolate nodes, or partition the cluster to prove the consensus
still stays safe.

Messages are queued with a delivery time (now + latency) and delivered when the
clock reaches it. No sockets, no real concurrency, fully deterministic.
"""

import heapq


class Network:
    def __init__(self, clock, latency=10):
        self.clock = clock       # the shared fake clock (to know "now")
        self.latency = latency   # delay before a message arrives (default 10)
        self.nodes = {}          # node_id -> Node (who's on the network)
        self._queue = []         # the heap of in-flight messages
        self._seq = 0            # a counter to break ties in the heap
        self.isolated = set()    # node ids fully cut off from everyone
        self.blocked = set()     # frozenset({a, b}) links that are down

    def register(self, node):
        self.nodes[node.id] = node

    # --- fault injection -------------------------------------------------
    def isolate(self, node_id):
        """Cut one node off from the whole cluster (models a crash/partition)."""
        self.isolated.add(node_id)

    def rejoin(self, node_id):
        """Undo isolate (reconnect a node)."""
        self.isolated.discard(node_id)

    def partition(self, group):
        """Sever every link between `group` and the rest of the cluster.

        Cut every connection between the group (e.g. {0,1}) and the rest
        (e.g. {2,3,4}) so the two sides can't talk. Nodes within each side
        still can.

        Adding this to make it easy to understand when presenting
        a = 0:
            b = 2 →  add frozenset({0,2})   → self.blocked now has {0,2}
            b = 3 →  add frozenset({0,3})
            b = 4 →  add frozenset({0,4})
        a = 1:
            b = 2 →  add frozenset({1,2})
            b = 3 →  add frozenset({1,3})
            b = 4 →  add frozenset({1,4})
        """
        rest = [n for n in self.nodes if n not in group]
        for a in group:
            for b in rest:
                # a frozenset is a set that can't be changed, so it can live
                # inside another set (self.blocked) as an unordered pair
                self.blocked.add(frozenset((a, b)))

    def heal(self):
        """Reconnect everyone (undo all isolate/partition)."""
        self.isolated.clear()
        self.blocked.clear()

    def _reachable(self, a, b):
        """Return True if a and b can talk, False if they can't."""
        if a in self.isolated or b in self.isolated:
            return False   # a node is isolated, so it can't reach anyone
        # check if a and b are allowed to talk to each other
        return frozenset((a, b)) not in self.blocked

    # --- message passing -------------------------------------------------
    def send(self, src, dst, msg):
        """Queue a message to arrive at (now + latency), or drop it if the
        link is down right now (like a lost packet)."""
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
