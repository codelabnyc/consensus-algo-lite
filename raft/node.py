"""A single Raft server.

Implements the two things that make Raft consensus work:

  1. Leader election -- randomized timeouts + a majority vote, with the `term`
     acting as a fencing token so stale leaders can't cause damage.
  2. Log replication -- the leader copies its ordered log to followers, commits
     an entry once a majority store it, and every node applies committed entries
     to its state machine in the same order.

The node is driven, not threaded: the harness calls `tick()` to let time pass
and `receive()` to deliver a message. This keeps the whole cluster deterministic.

Log indexing: `log[0]` is a sentinel (term 0) so real entries are 1-indexed,
matching the paper and avoiding off-by-one bugs.
"""

from dataclasses import dataclass, field
from typing import Any

FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"


@dataclass
class Entry:
    """One log entry: a command plus the term it was created in."""
    term: int
    command: Any


# --- RPC messages --------------------------------------------------------
@dataclass
class RequestVote:
    """A candidate asking its peers for a vote."""
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResp:
    """A peer's yes/no answer to a vote request."""
    term: int
    vote_granted: bool


@dataclass
class AppendEntries:
    """Leader -> follower: replicate entries (empty list == heartbeat)."""
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResp:
    """A follower's answer to AppendEntries (and how far its log now matches)."""
    term: int
    success: bool
    match_index: int = 0


class Node:
    def __init__(self, node_id, peers, network, clock, rng, state_machine,
                 min_election_timeout=150, max_election_timeout=300,
                 heartbeat_interval=50):
        self.id = node_id
        self.peers = list(peers)
        self.network = network
        self.clock = clock
        self.rng = rng           # seeded random number generator (election timeouts)
        self.sm = state_machine  # the state machine we replicate (key-value store)

        # In real Raft these 3 would be saved to disk so they survive a restart.
        # We keep them in memory to stay simple (saving to disk = beyond scope of this project).
        # If a node crashed and forgot these, it could break Raft's rules --
        # e.g. vote for two different candidates and wrongly create two leaders.
        # This is why a real-life implementation would save these somewhere persistent.
        self.current_term = 0
        self.voted_for = None
        self.log = [Entry(term=0, command=None)]  # index 0 = sentinel

        # Volatile state (values a node can safely rebuild after a restart).
        self.role = FOLLOWER
        self.commit_index = 0    # highest log entry known to be safely committed
        self.last_applied = 0    # highest log entry already applied to the KV store
        self.leader_id = None    # who we think the leader is (None if unknown)
        self.votes = set()       # who voted for me this election (only as candidate)

        # Leader-only volatile state (per-follower bookkeeping).
        self.next_index = {}     # for each follower: next log index to send them
        self.match_index = {}    # for each follower: highest index known to be on them

        # Timers.
        self.min_election_timeout = min_election_timeout  # min wait before an election
        self.max_election_timeout = max_election_timeout  # max wait (randomized in range)
        self.heartbeat_interval = heartbeat_interval  # how often the leader heartbeats
        self.next_heartbeat = 0.0  # when the leader should send its next heartbeat
        self.last_leader_contact = -1e9  # when we last heard from a leader (stickiness)
        self.up = True  # is this node alive? (False = crashed)
        self.reset_election_deadline()  # set the first random election deadline

    # --- small helpers ---------------------------------------------------
    def now(self):
        """Current time, read from the injected clock."""
        return self.clock.now()

    def last_log_index(self):
        """Index of the last log entry (0 when only the sentinel exists)."""
        return len(self.log) - 1

    def last_log_term(self):
        """Term of the last log entry."""
        return self.log[-1].term

    def majority(self):
        """How many nodes form a majority of the whole cluster."""
        return (len(self.peers) + 1) // 2 + 1

    def send(self, dst, msg):
        """Send a message to another node (fills in src = me)."""
        self.network.send(self.id, dst, msg)

    def reset_election_deadline(self):
        """Pick a fresh, randomized election timeout starting from now.

        Randomizing avoids everyone timing out together (split votes).
        """
        span = self.rng.uniform(self.min_election_timeout, self.max_election_timeout)
        self.election_deadline = self.now() + span

    # --- role transitions ------------------------------------------------
    def advance_term(self, term):
        """Adopt a newer term and revert to follower (the core fencing rule)."""
        self.current_term = term
        self.voted_for = None
        self.role = FOLLOWER
        self.leader_id = None
        self.reset_election_deadline()

    def become_leader(self):
        """Win the election: set up follower tracking and heartbeat immediately."""
        self.role = LEADER
        self.leader_id = self.id
        for p in self.peers:
            self.next_index[p] = self.last_log_index() + 1
            self.match_index[p] = 0
        self.match_index[self.id] = self.last_log_index()
        self.next_heartbeat = self.now()  # heartbeat on this tick

    def start_election(self):
        """Become a candidate, bump the term, and ask every peer for a vote."""
        self.role = CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.votes = {self.id}
        self.leader_id = None
        self.reset_election_deadline()
        li, lt = self.last_log_index(), self.last_log_term()
        for p in self.peers:
            self.send(p, RequestVote(self.current_term, self.id, li, lt))
        if len(self.votes) >= self.majority():  # single-node cluster
            self.become_leader()

    # --- driven by the harness ------------------------------------------
    def tick(self):
        """One time step: leaders heartbeat, others may start an election.

        Called by the harness once per simulated step. Also applies any
        newly-committed entries to the state machine.
        """
        if not self.up:
            return
        now = self.now()
        if self.role == LEADER:
            if now >= self.next_heartbeat:
                for p in self.peers:
                    self.send_append_entries(p)
                self.next_heartbeat = now + self.heartbeat_interval
        elif now >= self.election_deadline:
            self.start_election()
        self.apply_committed()

    def receive(self, src, msg):
        """Dispatch an incoming message to the matching handler."""
        if not self.up:
            return
        if isinstance(msg, RequestVote):
            self.handle_request_vote(src, msg)
        elif isinstance(msg, RequestVoteResp):
            self.handle_request_vote_resp(src, msg)
        elif isinstance(msg, AppendEntries):
            self.handle_append_entries(src, msg)
        elif isinstance(msg, AppendEntriesResp):
            self.handle_append_entries_resp(src, msg)

    # --- elections -------------------------------------------------------
    def handle_request_vote(self, src, m):
        """Decide whether to grant a candidate our vote."""
        # Leader stickiness: if a leader is still alive to us, refuse to vote.
        # This stops a node cut off from the leader (but not from us) from
        # forcing a needless election. We reject WITHOUT adopting the higher
        # term, which also prevents the term inflation that would otherwise
        # depose a healthy leader -- a lightweight stand-in for Pre-Vote.
        if (self.leader_id is not None
                and self.now() - self.last_leader_contact < self.min_election_timeout):
            self.send(src, RequestVoteResp(self.current_term, False))
            return

        if m.term < self.current_term:
            self.send(src, RequestVoteResp(self.current_term, False))
            return
        if m.term > self.current_term:
            self.advance_term(m.term)

        # Only vote for a candidate whose log is at least as up-to-date as ours
        # (compare last term, then last index). This guarantees a new leader
        # already holds every committed entry.
        up_to_date = (m.last_log_term > self.last_log_term()
                      or (m.last_log_term == self.last_log_term()
                          and m.last_log_index >= self.last_log_index()))
        if self.voted_for in (None, m.candidate_id) and up_to_date:
            self.voted_for = m.candidate_id
            self.reset_election_deadline()
            self.send(src, RequestVoteResp(self.current_term, True))
        else:
            self.send(src, RequestVoteResp(self.current_term, False))

    def handle_request_vote_resp(self, src, m):
        """Count a vote reply; become leader once a majority say yes."""
        if m.term > self.current_term:
            self.advance_term(m.term)
            return
        if self.role != CANDIDATE or m.term != self.current_term:
            return  # stale reply
        if m.vote_granted:
            self.votes.add(src)
            if len(self.votes) >= self.majority():
                self.become_leader()

    # --- log replication -------------------------------------------------
    def send_append_entries(self, dst):
        """Send a follower the entries it's missing (empty == heartbeat)."""
        ni = self.next_index.get(dst, self.last_log_index() + 1)
        prev_index = ni - 1
        prev_term = self.log[prev_index].term
        entries = self.log[ni:]  # empty list == pure heartbeat
        self.send(dst, AppendEntries(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=list(entries),
            leader_commit=self.commit_index,
        ))

    def handle_append_entries(self, src, m):
        """Handle the leader's replication/heartbeat; append entries, update commit."""
        if m.term < self.current_term:
            self.send(src, AppendEntriesResp(self.current_term, False))
            return
        if m.term > self.current_term:
            self.advance_term(m.term)

        # Valid leader for this term: follow it and reset our election timer.
        self.role = FOLLOWER
        self.leader_id = m.leader_id
        self.last_leader_contact = self.now()
        self.reset_election_deadline()

        # Consistency check: our log must match at prev_log_index.
        if (m.prev_log_index > self.last_log_index()
                or self.log[m.prev_log_index].term != m.prev_log_term):
            self.send(src, AppendEntriesResp(self.current_term, False))
            return

        # Append new entries, overwriting any divergent tail.
        idx = m.prev_log_index
        for e in m.entries:
            idx += 1
            if idx <= self.last_log_index():
                if self.log[idx].term != e.term:
                    del self.log[idx:]
                    self.log.append(e)
            else:
                self.log.append(e)

        if m.leader_commit > self.commit_index:
            self.commit_index = min(m.leader_commit, self.last_log_index())
        self.apply_committed()
        self.send(src, AppendEntriesResp(
            self.current_term, True, m.prev_log_index + len(m.entries)))

    def handle_append_entries_resp(self, src, m):
        """Handle a follower's reply: advance commit on success, or retry further back."""
        if m.term > self.current_term:
            self.advance_term(m.term)
            return
        if self.role != LEADER or m.term != self.current_term:
            return
        if m.success:
            self.match_index[src] = m.match_index
            self.next_index[src] = m.match_index + 1
            self.maybe_advance_commit()
        else:
            self.next_index[src] = max(1, self.next_index.get(src, 1) - 1)
            self.send_append_entries(src)  # retry further back

    def maybe_advance_commit(self):
        """Commit the highest entry stored on a majority (own-term entries only)."""
        # Advance commit_index to the highest N replicated on a majority -- but
        # only for entries from our own term (Raft's commitment rule, which
        # fixes a subtle safety bug from the paper's first version).
        for n in range(self.last_log_index(), self.commit_index, -1):
            if self.log[n].term != self.current_term:
                continue
            count = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if count >= self.majority():
                self.commit_index = n
                break

    def apply_committed(self):
        """Apply newly-committed entries to the state machine, in log order."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.sm.apply(self.log[self.last_applied].command)

    # --- client entry point ---------------------------------------------
    def client_submit(self, command):
        """Submit a command. Returns its log index, or None if not the leader."""
        if self.role != LEADER:
            return None
        self.log.append(Entry(self.current_term, command))
        self.match_index[self.id] = self.last_log_index()
        for p in self.peers:
            self.send_append_entries(p)
        return self.last_log_index()
