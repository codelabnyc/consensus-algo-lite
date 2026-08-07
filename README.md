# Toy Raft — Consensus & Replicated Key-Value Store

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
in Python, with a replicated key-value store built on top.

Built for a backend take-home exercise: demonstrate an understanding of how
distributed systems achieve **consensus** and replicate **state** reliably.

## Why Raft

Raft solves consensus: getting a cluster of servers to agree on an ordered log
of commands, even when nodes crash or messages are lost. It was designed to be
*understandable* (unlike Paxos), which makes it the right choice for an exercise
where reasoning matters more than raw functionality.

Consensus gives us **state-machine replication**: every node applies the same
commands in the same order, so they all reach the same state. The state machine
here is a simple key-value store.

## Scope

**Implemented**
- Leader election (randomized timeouts, majority vote, terms)
- Log replication (append, replicate to majority, commit, apply)
- A key-value state machine on top of the committed log

**Deliberately deferred (future work)**
- Persistence to disk
- Log compaction / snapshotting
- Cluster membership changes

These are real work but not needed to demonstrate the core ideas.

## Design

Consensus only agrees on the ordered **log**. A separate step applies each
committed entry to the state machine. The two are decoupled — the KV store is
just one pluggable state machine.

Tests run the whole cluster **in one process on a simulated clock and network**,
so we can crash nodes, drop messages, and assert invariants deterministically —
no real sockets, no flaky timing.

## Getting started

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest
```

## Layout

```
raft/
  clock.py      # manually-advanced clock (deterministic time)
  network.py    # in-process message bus with fault injection
  node.py       # a Raft server: election + log replication
  kv.py         # the replicated state machine (key-value store)
  cluster.py    # test harness wiring nodes + clock + network together
tests/
  test_network.py      # clock/network smoke test
  test_election.py     # leader election + safety
  test_replication.py  # log replication, ordering, catch-up
  test_kv.py           # read-after-write across the cluster
  test_faults.py       # partitions, availability, log repair
  invariants.py        # shared Raft safety checks
```

## Key design decisions (the "why")

- **Deterministic simulation over real sockets.** The whole cluster runs in one
  process on a manual clock. This makes failure tests (crash the leader, drop
  messages, partition the network) exact and repeatable instead of flaky. The
  assignment values testability, so the harness is a first-class component.
- **Consensus decoupled from the application.** `node.py` only agrees on an
  ordered log; `kv.py` is a plain dict it applies committed entries to. Any
  deterministic state machine could be swapped in.
- **The `term` is the fencing token.** Every RPC carries a term; a higher term
  seen means step down, a lower term seen means reject. This is what other
  systems call the *epoch* (Kafka/ZAB) or *ballot* (Paxos).
- **Leader stickiness instead of naive voting.** A follower that recently heard
  from its leader refuses to vote, *without* adopting the candidate's term. This
  stops a node cut off from the leader (but not from its peers) from deposing a
  healthy leader — a lightweight stand-in for full Pre-Vote.
- **1-indexed log with a sentinel** at index 0, to match the paper and avoid
  off-by-one errors in the consistency check.

## Safety invariants the tests assert

- **Election safety** — at most one leader per term (majority vote + one vote
  per term; two majorities must overlap).
- **Log matching** — same index + same term implies identical history.
- **State-machine safety** — no two nodes apply different commands at the same
  index.
- **Leader completeness** — a committed entry survives leader changes (enforced
  by the up-to-date check during voting).

## Deferred (future work)

Deliberately out of scope; each is real work not needed to demonstrate the core:

- Persistence to disk (term/vote/log are in memory, so a true crash-restart is
  unsafe — this is *why* persistence matters).
- Log compaction / snapshotting.
- Cluster membership changes.
- Full **Pre-Vote** (approximated by leader stickiness) and read-index reads.
