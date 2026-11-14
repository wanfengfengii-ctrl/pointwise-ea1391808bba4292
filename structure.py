"""Structural verification for the refactored responsibility boundaries.

Exercises the real production modules: pure rules in isolation, merge
scheduling over opaque cells and a substitute port driver, driver ownership
under the workspace fault facility, and static import/call boundaries.
Run directly or through verify_all.py.
"""
import ast
from pathlib import Path

import driver as driver_module
import pipeline
import reduction
import sorter
import spill
from workspace import MemoryWorkspace

ROOT = Path(__file__).resolve().parent


def check_rules():
    rules = reduction.Rules()
    record = ["g", "dev", 7]
    cell = rules.lift(record)
    assert cell == ["g", "dev", 1, 7] and cell is not record
    assert record == ["g", "dev", 7]
    for bad in (["g", "dev"], ["g", "dev", 1, 2], [1, "d", 2], ["g", 2, 2],
                ["g", "d", True], ["g", "d", 1.5], "abc", None):
        try:
            rules.lift(bad)
        except ValueError as exc:
            assert str(exc) == "record"
        else:
            raise AssertionError("bad record accepted: %r" % (bad,))
    assert rules.key(["g", None, 1, 1]) < rules.key(["g", "", 1, 1])
    left, right = ["g", "dev", 2, 5], ["g", "dev", 3, -1]
    merged = rules.combine(left, right)
    assert merged == ["g", "dev", 5, 4]
    assert left == ["g", "dev", 2, 5] and right == ["g", "dev", 3, -1]
    # Same device across segments shares one key, so it is counted once.
    across = rules.combine(["a", "x", 1, 2], ["a", "x", 2, 3])
    cells = [across, ["a", None, 1, 4], ["a", "", 1, -1], ["b", "y", 1, 1]]
    snapshot = [list(c) for c in cells]
    out = rules.finish(iter(cells))
    assert not isinstance(out, list) and hasattr(out, "close")
    assert next(out) == ("a", 5, 8, 2)
    assert next(out) == ("b", 1, 1, 1)
    try:
        next(out)
    except StopIteration:
        pass
    else:
        raise AssertionError("finish did not terminate")
    assert cells == snapshot
    assert vars(rules) == {}


class Opaque:
    """Cell double without subscripting, ordering or equality."""
    __slots__ = ("payload",)

    def __init__(self, payload):
        self.payload = payload


class WordRules:
    """Custom rules over opaque cells; finish must stay unused by order."""

    def lift(self, record):
        return Opaque((record[0], record[1]))

    def key(self, cell):
        return cell.payload[0]

    def combine(self, left, right):
        return Opaque((left.payload[0], left.payload[1] + right.payload[1]))

    def finish(self, cells):
        raise AssertionError("order must not call finish")


class CloseTracker:
    def __init__(self, rows):
        self.iterator = iter(rows)
        self.pulls = 0
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        self.pulls += 1
        return next(self.iterator)

    def close(self):
        self.closed = True


class PortDriver:
    """Segment double exposing only the driver port, backed by dicts."""

    def __init__(self):
        self.segments = {}
        self.serial = 0
        self.positions = {}
        self.buffers = {}
        self.removed = []
        self.read_opens = {}
        self.cleanup_called = False

    def allocate(self):
        self.serial += 1
        return "seg%04d" % self.serial

    def open_read(self, name):
        self.read_opens[name] = self.read_opens.get(name, 0) + 1
        token = ("r", name)
        self.positions[token] = 0
        return token

    def open_write(self, name):
        token = ("w", name)
        self.buffers[token] = []
        return token

    def read(self, token):
        pos = self.positions[token]
        data = self.segments.get(token[1], [])
        if pos >= len(data):
            return None
        self.positions[token] = pos + 1
        return data[pos]

    def write(self, token, cell):
        self.buffers[token].append(cell)

    def close(self, token):
        kind, name = token
        if kind == "w":
            self.segments[name] = self.buffers.pop(token, [])
        else:
            self.positions.pop(token, None)

    def remove(self, name):
        self.removed.append(name)
        self.segments.pop(name, None)

    def cleanup(self):
        self.cleanup_called = True
        return []


def check_order():
    rows = [("b", 1), ("a", 2), ("b", 3), ("a", 4), ("c", 5)]
    serials = {(1, 2): 10, (1, 3): 7, (2, 2): 5, (2, 3): 3, (3, 2): 2, (3, 3): 2}
    for size in (1, 2, 3):
        for fanin in (2, 3):
            driver = PortDriver()
            source = CloseTracker(rows)
            cells = sorter.order(source, driver, WordRules(), size, fanin)
            pulled = [cell.payload for cell in cells]
            assert pulled == [("a", 6), ("b", 4), ("c", 5)], (size, fanin, pulled)
            assert source.pulls == len(rows) + 1 and not source.closed
            assert not driver.cleanup_called
            assert driver.serial == serials[(size, fanin)], (size, fanin)
            allocated = {"seg%04d" % i for i in range(1, driver.serial + 1)}
            remaining = set(driver.segments)
            removed = set(driver.removed)
            assert remaining and removed.isdisjoint(remaining)
            assert removed | remaining == allocated
            assert len(driver.removed) == len(removed)
            assert all(count == 1 for count in driver.read_opens.values())
            assert set(driver.read_opens) == allocated


def check_driver():
    ws = MemoryWorkspace(fail={"close": [1], "remove": [1]})
    drv = driver_module.Driver(ws)
    name = drv.allocate()
    assert ws.trace == [] and ws.remaining() == []
    token = drv.open_write(name)
    drv.write(token, ["g", "d", 1, 1])
    try:
        drv.close(token)
    except OSError as exc:
        assert str(exc) == "close:1"
    else:
        raise AssertionError("close fault not propagated")
    assert ws.live == [] and token not in drv.handles
    try:
        drv.remove(name)
    except OSError as exc:
        assert str(exc) == "remove:1"
    else:
        raise AssertionError("remove fault not propagated")
    assert ws.remaining() == [] and drv.owned == []

    ws2 = MemoryWorkspace(fail={"close": [1, 2], "remove": [1, 2]})
    drv2 = driver_module.Driver(ws2)
    first, second = drv2.allocate(), drv2.allocate()
    t1 = drv2.open_write(first)
    drv2.write(t1, ["a", None, 1, 1])
    t2 = drv2.open_write(second)
    drv2.write(t2, ["b", "x", 1, 2])
    errors = drv2.cleanup()
    assert [str(e) for e in errors] == ["close:1", "close:2", "remove:1", "remove:2"]
    assert all(type(e) is OSError for e in errors)
    assert ws2.live == [] and ws2.remaining() == []
    assert drv2.handles == {} and drv2.owned == []
    ops = [event[0:2] for event in ws2.trace]
    assert ops[-4:] == [["close", second], ["close", first],
                        ["remove", second], ["remove", first]]
    assert driver_module.Driver(MemoryWorkspace()).cleanup() == []


def module_imports(filename):
    tree = ast.parse((ROOT / filename).read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module.split(".")[0])
    return found


def attributes_on(filename, name):
    tree = ast.parse((ROOT / filename).read_text(encoding="utf-8"))
    return {node.attr for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name) and node.value.id == name}


def check_boundaries():
    allowed = {
        "cleanup.py": set(),
        "reduction.py": set(),
        "sorter.py": {"heapq", "cleanup"},
        "driver.py": {"cleanup"},
        "pipeline.py": {"cleanup", "driver", "reduction", "sorter"},
        "spill.py": {"pipeline"},
    }
    for filename, permitted in allowed.items():
        found = module_imports(filename)
        assert found <= permitted, (filename, found)
        assert "workspace" not in found and "checks" not in found
        text = (ROOT / filename).read_text(encoding="utf-8")
        for banned in ("__import__", "eval(", "exec(", "importlib"):
            assert banned not in text, (filename, banned)
    assert module_imports("sorter.py") == {"heapq", "cleanup"}
    port = {"allocate", "open_read", "open_write", "read", "write",
            "close", "remove"}
    assert attributes_on("sorter.py", "driver") <= port
    assert attributes_on("pipeline.py", "workspace") == set()
    assert spill.aggregate is pipeline.aggregate
    assert pipeline.order is sorter.order
    assert pipeline.Rules is reduction.Rules
    assert pipeline.Driver is driver_module.Driver


def verify():
    check_rules()
    check_order()
    check_driver()
    check_boundaries()
    print("STRUCTURE_OK")


if __name__ == "__main__":
    verify()
