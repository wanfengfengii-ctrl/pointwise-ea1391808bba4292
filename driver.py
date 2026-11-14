"""Segment resource driver: sole owner of real handles and owned segments.

The scheduler sees only opaque hashable tokens and segment names; the real
Handle never leaves this module. Explicit close/remove release the matching
registration even when injected faults fire, so nothing is released twice.
cleanup walks still-registered handles and segments in reverse order, item
by item, and returns the collected exceptions without replacing any primary
exception. No key comparison, no reduction, no cell-stream traversal here.
"""
from cleanup import collect


class Driver:
    def __init__(self, workspace):
        self.workspace = workspace
        self.owned = []
        self.handles = {}

    def allocate(self):
        name = self.workspace.allocate()
        self.owned.append(name)
        return name

    def _open(self, name, mode):
        handle = self.workspace.open(name, mode)
        token = object()
        self.handles[token] = handle
        return token

    def open_read(self, name):
        return self._open(name, "r")

    def open_write(self, name):
        return self._open(name, "w")

    def read(self, token):
        return self.handles[token].read()

    def write(self, token, cell):
        self.handles[token].write(cell)

    def close(self, token):
        handle = self.handles.pop(token, None)
        if handle is not None:
            handle.close()

    def remove(self, name):
        self.owned.remove(name)
        self.workspace.remove(name)

    def cleanup(self):
        actions = [lambda token=token: self.close(token)
                   for token in reversed(list(self.handles))]
        actions += [lambda name=name: self.remove(name)
                    for name in reversed(list(self.owned))]
        return collect(actions)
