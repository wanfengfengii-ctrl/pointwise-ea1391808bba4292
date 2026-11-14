import hashlib
import json
import random
import sys
import tempfile
from pathlib import Path

from spill import aggregate
from workspace import Workspace, MemoryWorkspace

ROOT = Path(__file__).resolve().parent


class Input:
    def __init__(self, rows, trace, fail_at=None, close_fail=False):
        self.rows, self.trace, self.index = rows, trace, 0
        self.fail_at, self.close_fail, self.closed = fail_at, close_fail, False

    def __iter__(self):
        return self

    def __next__(self):
        self.trace.append(["pull", self.index])
        if self.index == self.fail_at:
            raise RuntimeError("input:" + str(self.index))
        if self.index == len(self.rows):
            raise StopIteration
        row = self.rows[self.index]
        self.index += 1
        return row

    def close(self):
        if not self.closed:
            self.closed = True
            self.trace.append(["input_close"])
            if self.close_fail:
                raise RuntimeError("input_close")


def sample():
    return json.loads((ROOT / "samples.json").read_text(encoding="utf-8"))


def reference(rows):
    groups = {}
    for group, device, amount in rows:
        count, total, devices = groups.setdefault(group, [0, 0, set()])
        if device is not None:
            devices.add(device)
        groups[group] = [count + 1, total + amount, devices]
    return [(g, c, s, len(d)) for g, (c, s, d) in sorted(groups.items())]


def scenario(rows, size=2, fanin=2, fail=None, stop=None,
             input_fail=None, close_fail=False, late=False, workspace=None):
    trace = []
    ws = workspace if workspace is not None else MemoryWorkspace()
    ws.trace = trace
    ws.fail = fail or {}
    source = Input(rows, trace, input_fail, close_fail)
    it = aggregate(source, ws, size, fanin)
    assert trace == [] and source.index == 0
    output, marks, error = [], [], None
    try:
        if stop == 0:
            it.close()
        else:
            while True:
                output.append(next(it))
                marks.append(len(trace))
                if late:
                    ws.fail["read"] = [ws.counts.get("read", 0) + 1]
                    late = False
                if stop is not None and len(output) == stop:
                    it.close()
                    break
    except StopIteration:
        pass
    except Exception as exc:
        error = [type(exc).__name__, str(exc)]
    assert ws.live == [], ws.live
    assert ws.remaining() == [], ws.remaining()
    assert ws.peak <= fanin + 1, ws.peak
    return dict(output=output, marks=marks, error=error, trace=trace)


def cases():
    rows = sample()["rows"]
    return {
        "normal": scenario(rows),
        "empty": scenario([]),
        "unstarted": scenario(rows, stop=0),
        "early": scenario(rows, stop=1),
        "input": scenario(rows, input_fail=5, close_fail=True),
        "write": scenario(rows, fail={"write": [2], "close": [1], "remove": [1]}),
        "read": scenario(rows, fail={"read": [3], "close": [9], "remove": [1]}),
        "open": scenario(rows, fail={"open_r": [2]}),
        "close": scenario(rows, fail={"close": [1]}),
        "remove": scenario(rows, fail={"remove": [1, 3]}),
        "input_close": scenario(rows, close_fail=True),
        "late_read": scenario(rows, late=True),
        "invalid": scenario([["a", "x", 1], ["a", None, True]]),
        "unicode": scenario([["", "", 0], ["中", None, -2], ["中", "", 4],
                             ["中", "", -4], ["a", None, 7]], 1, 2),
    }


def digest(value):
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def verify():
    observed = cases()
    expected = json.loads((ROOT / "expected.json").read_text(encoding="utf-8"))
    assert {k: digest(v) for k, v in observed.items()} == expected
    assert observed["normal"]["output"] == reference(sample()["rows"])
    assert [list(r) for r in observed["normal"]["output"]] == sample()["expected"]
    assert observed["unstarted"] == dict(output=[], marks=[], error=None, trace=[])
    assert observed["read"]["error"] == ["OSError", "read:3"]
    assert observed["write"]["error"] == ["OSError", "write:2"]
    assert observed["input"]["error"] == ["RuntimeError", "input:5"]
    assert observed["late_read"]["output"] == [("a", 6, 11, 3)]
    assert observed["input_close"]["output"] == reference(sample()["rows"])
    assert observed["input_close"]["error"] == ["RuntimeError", "input_close"]
    rng = random.Random(117)
    for size in (1, 2, 5, 17):
        for fanin in (2, 3, 8):
            for n in (0, 1, 7, 65):
                rows = [[rng.choice(["a", "b", "", "中"]), rng.choice([None, "", "x", "y"]),
                         rng.randrange(-9, 10)] for _ in range(n)]
                result = scenario(rows, size, fanin)
                assert result["error"] is None
                assert result["output"] == reference(rows)
    for size, fanin in [(0, 2), (True, 2), (2, 1), (2, False)]:
        ws = MemoryWorkspace()
        try:
            aggregate([], ws, size, fanin)
        except ValueError:
            assert ws.trace == []
        else:
            raise AssertionError("bad limits accepted")
    print("BEHAVIOR_REGRESSION_OK")


def disk():
    with tempfile.TemporaryDirectory(prefix="warehouse-spill-") as root:
        ws = Workspace(root)
        result = scenario(sample()["rows"], workspace=ws)
        assert result["error"] is None
        assert result["output"] == reference(sample()["rows"])
        assert digest(result) == json.loads((ROOT / "expected.json").read_text())["normal"]
        assert list(Path(root).iterdir()) == []
        print(json.dumps({"output": result["output"], "trace": result["trace"],
                          "remaining": ws.remaining()}, ensure_ascii=False, indent=2))


def demo():
    result = scenario(sample()["rows"])
    print(json.dumps({"output": result["output"], "trace": result["trace"],
                      "error": result["error"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "verify"
    {"verify": verify, "demo": demo, "disk": disk}[mode]()
