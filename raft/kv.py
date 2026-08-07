"""The key-value store: applies commands (put / delete / get).

Just a wrapper around a dictionary. node.py decides the order of commands;
this file only applies them. Kept trivial on purpose -- the hard part is the
replication, not the storage.
"""


class KV:
    def __init__(self):
        self.data = {}

    def apply(self, command):
        op = command["op"]
        if op == "put":
            self.data[command["key"]] = command["value"]
        elif op == "delete":
            self.data.pop(command["key"], None)
        else:
            raise ValueError(f"unknown op: {op!r}")
        return self.data.get(command.get("key"))

    def get(self, key):
        return self.data.get(key)
