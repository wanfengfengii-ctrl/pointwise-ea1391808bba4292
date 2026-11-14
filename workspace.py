"""Segment I/O and deterministic fault recording; no aggregation rules."""
import io
import json
from pathlib import Path


class Handle:
    def __init__(self, owner, name, mode, stream):
        self.owner, self.name, self.mode, self.stream = owner, name, mode, stream
        self.closed = False

    def read(self):
        self.owner.tick("read", self.name)
        line = self.stream.readline()
        return json.loads(line) if line else None

    def write(self, cell):
        self.owner.tick("write", self.name, cell)
        self.stream.write(json.dumps(cell, ensure_ascii=False) + "\n")

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self.stream.close()
            finally:
                self.owner.live.remove(self)
            self.owner.tick("close", self.name, self.mode)


class Workspace:
    """root is an existing, dedicated empty directory; one aggregation at a time."""
    def __init__(self, root, trace=None, fail=None):
        self.root = Path(root)
        self.trace = trace if trace is not None else []
        self.fail = fail or {}
        self.counts, self.live, self.serial, self.peak = {}, [], 0, 0

    def tick(self, op, *args):
        self.trace.append(json.loads(json.dumps([op, *args], ensure_ascii=False)))
        n = self.counts.get(op, 0) + 1
        self.counts[op] = n
        if n in self.fail.get(op, []):
            raise OSError(op + ":" + str(n))

    def allocate(self):
        self.serial += 1
        return "r%04d" % self.serial

    def _open(self, name, mode):
        return (self.root / name).open(mode, encoding="utf-8", newline="")

    def open(self, name, mode):
        self.tick("open_" + mode, name)
        handle = Handle(self, name, mode, self._open(name, mode))
        self.live.append(handle)
        self.peak = max(self.peak, len(self.live))
        return handle

    def remove(self, name):
        path = self.root / name
        if path.exists():
            path.unlink()
        self.tick("remove", name)

    def remaining(self):
        return sorted(p.name for p in self.root.glob("r[0-9][0-9][0-9][0-9]*"))


class MemoryWorkspace(Workspace):
    """Test double for the same file operations, not an aggregation implementation."""
    def __init__(self, trace=None, fail=None):
        super().__init__(".", trace, fail)
        self.data = {}

    def _open(self, name, mode):
        if mode == "r":
            return io.StringIO(self.data[name])
        self.data[name] = ""
        owner = self

        class Writer(io.StringIO):
            def close(self):
                if not self.closed:
                    owner.data[name] = self.getvalue()
                super().close()
        return Writer()

    def remove(self, name):
        self.data.pop(name, None)
        self.tick("remove", name)

    def remaining(self):
        return sorted(self.data)
