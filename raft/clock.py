"""A clock we control by hand.

Real nodes each use their own local clock -- Raft only needs local timeouts
("has 150ms passed since I heard from the leader?"), not clocks that agree
across machines. But our whole cluster runs inside one process in one test, so
reading the real computer clock would make tests slow, flaky, and impossible to
reproduce. Instead we inject this clock, where time is just a number that only
moves when we call advance(). Nothing ever sleeps, and the same test runs the
same way every time.
"""


class Clock:
    def __init__(self, start=0.0):
        self._now = start

    def now(self):
        return self._now

    def advance(self, dt):
        """Move time forward by dt (delta time)."""
        if dt < 0:
            raise ValueError("time only moves forward")
        self._now += dt
        return self._now
