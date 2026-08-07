"""Shared invariant checks used across the test suite.

These encode Raft's safety properties so tests can assert them directly.
"""

from raft.node import LEADER


def one_leader_per_term(observed):
    """`observed` maps term -> set of node ids seen as leader in that term.

    Election safety: a term can have at most one leader, ever.
    """
    return all(len(ids) <= 1 for ids in observed.values())


def record_leaders(cluster, observed):
    """Snapshot current leaders into `observed` (term -> set of ids)."""
    for node in cluster.nodes.values():
        if node.role == LEADER:
            observed.setdefault(node.current_term, set()).add(node.id)


def committed_prefixes_agree(nodes):
    """No two nodes hold different entries at the same committed index.

    (Log-matching + state-machine safety.) Compares the overlap of committed
    regions across the given nodes.
    """
    for i in range(len(nodes)):
        for j in range(i + 1, len(nodes)):
            a, b = nodes[i], nodes[j]
            upto = min(a.commit_index, b.commit_index)
            for idx in range(1, upto + 1):
                if a.log[idx].term != b.log[idx].term:
                    return False
                if a.log[idx].command != b.log[idx].command:
                    return False
    return True
