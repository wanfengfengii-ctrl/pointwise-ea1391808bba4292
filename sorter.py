"""External merge scheduling: bounded chunks, spill rounds, k-way heap merge.

Segments are reached only through the driver port; records and cells are
interpreted only through rules.lift/key/combine. The scheduler holds segment
names, opaque tokens, cells and sort keys, never physical handles. It does
not close the raw input and never calls driver.cleanup: the outer lifecycle
owns the input and the final segments until the public output completes.
"""
import heapq

from cleanup import clean


def _collapse(cells, rules):
    pending = None
    for cell in cells:
        if pending is not None and rules.key(pending) == rules.key(cell):
            pending = rules.combine(pending, cell)
        else:
            if pending is not None:
                yield pending
            pending = cell
    if pending is not None:
        yield pending


def _merge(driver, rules, names):
    tokens, heap = [], []
    failed = False
    try:
        for index, name in enumerate(names):
            token = driver.open_read(name)
            tokens.append(token)
            cell = driver.read(token)
            if cell is not None:
                heapq.heappush(heap, (rules.key(cell), index, cell))
        pending = None
        while heap:
            _, index, cell = heapq.heappop(heap)
            following = driver.read(tokens[index])
            if following is not None:
                heapq.heappush(heap, (rules.key(following), index, following))
            if pending is not None and rules.key(pending) == rules.key(cell):
                pending = rules.combine(pending, cell)
            else:
                if pending is not None:
                    yield pending
                pending = cell
        if pending is not None:
            yield pending
    except BaseException:
        failed = True
        raise
    finally:
        clean([lambda token=token: driver.close(token)
               for token in reversed(tokens)], failed)


def _save(driver, rules, cells):
    name = driver.allocate()
    token = None
    failed = False
    try:
        token = driver.open_write(name)
        for cell in cells:
            driver.write(token, cell)
    except BaseException:
        failed = True
        raise
    finally:
        clean([getattr(cells, "close", None),
               (lambda: driver.close(token)) if token is not None else None],
              failed)
    return name


def order(records, driver, rules, rows_per_run, fanin):
    """Yield ordered, same-key-combined cells pulled once from records."""
    runs, chunk = [], []
    for record in records:
        chunk.append(rules.lift(record))
        if len(chunk) == rows_per_run:
            chunk.sort(key=rules.key)
            runs.append(_save(driver, rules, _collapse(chunk, rules)))
            chunk = []
    if chunk:
        chunk.sort(key=rules.key)
        runs.append(_save(driver, rules, _collapse(chunk, rules)))
    while len(runs) > fanin:
        following = []
        for start in range(0, len(runs), fanin):
            part = runs[start:start + fanin]
            following.append(_save(driver, rules, _merge(driver, rules, part)))
            for name in part:
                driver.remove(name)
        runs = following
    yield from _merge(driver, rules, runs)
