"""Working legacy external aggregate. The nested ownership and rules are intentional."""
import heapq


def aggregate(records, workspace, rows_per_run=1000, fanin=8):
    if type(rows_per_run) is not int or rows_per_run < 1:
        raise ValueError("rows_per_run")
    if type(fanin) is not int or fanin < 2:
        raise ValueError("fanin")

    def execute():
        owned = []
        source = None

        def clean(actions, failed):
            errors = []
            for action in actions:
                if action is not None:
                    try:
                        action()
                    except BaseException as error:
                        errors.append(error)
            if errors and not failed:
                raise errors[0]

        def retire(name):
            owned.remove(name)
            workspace.remove(name)

        def key(cell):
            return cell[0], cell[1] is not None, cell[1] or ""

        def collapse(cells):
            pending = None
            for cell in cells:
                if pending is not None and key(pending) == key(cell):
                    pending = [pending[0], pending[1],
                               pending[2] + cell[2], pending[3] + cell[3]]
                else:
                    if pending is not None:
                        yield pending
                    pending = cell
            if pending is not None:
                yield pending

        def merge(names):
            handles, heap = [], []
            failed = False
            try:
                for index, name in enumerate(names):
                    handle = workspace.open(name, "r")
                    handles.append(handle)
                    cell = handle.read()
                    if cell is not None:
                        heapq.heappush(heap, (key(cell), index, cell))
                pending = None
                while heap:
                    _, index, cell = heapq.heappop(heap)
                    following = handles[index].read()
                    if following is not None:
                        heapq.heappush(heap, (key(following), index, following))
                    if pending is not None and key(pending) == key(cell):
                        pending = [pending[0], pending[1],
                                   pending[2] + cell[2], pending[3] + cell[3]]
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
                clean([h.close for h in reversed(handles)], failed)

        def save(cells):
            name = workspace.allocate()
            owned.append(name)
            handle = None
            failed = False
            try:
                handle = workspace.open(name, "w")
                for cell in cells:
                    handle.write(cell)
            except BaseException:
                failed = True
                raise
            finally:
                clean([getattr(cells, "close", None),
                       handle.close if handle is not None else None], failed)
            return name

        failed = False
        final = None
        try:
            source = iter(records)
            runs, chunk = [], []
            for record in source:
                if (not isinstance(record, (tuple, list)) or len(record) != 3
                    or not isinstance(record[0], str)
                    or not (record[1] is None or isinstance(record[1], str))
                    or type(record[2]) is not int):
                    raise ValueError("record")
                chunk.append([record[0], record[1], 1, record[2]])
                if len(chunk) == rows_per_run:
                    chunk.sort(key=key)
                    runs.append(save(collapse(chunk)))
                    chunk = []
            if chunk:
                chunk.sort(key=key)
                runs.append(save(collapse(chunk)))
            while len(runs) > fanin:
                following = []
                for start in range(0, len(runs), fanin):
                    part = runs[start:start + fanin]
                    following.append(save(merge(part)))
                    for name in part:
                        retire(name)
                runs = following
            final = merge(runs)
            group, count, amount, devices = None, 0, 0, 0
            for cell in final:
                if group is not None and group != cell[0]:
                    yield (group, count, amount, devices)
                    count, amount, devices = 0, 0, 0
                group = cell[0]
                count += cell[2]
                amount += cell[3]
                devices += cell[1] is not None
            if group is not None:
                yield (group, count, amount, devices)
        except BaseException:
            failed = True
            raise
        finally:
            actions = [getattr(source, "close", None), getattr(final, "close", None)]
            actions += [h.close for h in reversed(list(workspace.live))]
            actions += [lambda name=n: retire(name) for n in reversed(list(owned))]
            clean(actions, failed)
    return execute()
