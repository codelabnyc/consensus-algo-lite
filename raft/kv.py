"""The key-value store: applies commands (put / delete / get).

Just a wrapper around a dictionary. node.py decides the order of commands;
this file only applies them. Kept trivial on purpose -- the hard part is the
replication, not the storage.
"""


class KV:
    def __init__(self):
        self.data = {}

    def apply(self, command):
        """Run one command against the store.

        `command` is a dict describing the operation, e.g.
            {"op": "put", "key": "x", "value": 1}   -> set x = 1
            {"op": "delete", "key": "x"}            -> remove x
        """
        op = command["op"]
        if op == "put":
            self.data[command["key"]] = command["value"]
        elif op == "delete":
            self.data.pop(command["key"], None)
        else:
            raise ValueError(f"unknown op: {op!r}")
        return self.data.get(command.get("key"))

    def get(self, key):
        """Read a key's value, or None if it's missing."""
        return self.data.get(key)
