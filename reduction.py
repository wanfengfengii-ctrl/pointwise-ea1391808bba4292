"""Pure business reduction rules for the external group aggregate.

Rules owns only the business contract: record validation and lifting, cell
ordering keys, same-key combining and the streaming output reduction.
No files, workspaces, handles, I/O callbacks or mutable global state.
"""


class Rules:
    """Stateless rule set; methods only read values and return new ones."""

    def lift(self, record):
        """Validate one input row and return a new segment cell."""
        if (not isinstance(record, (tuple, list)) or len(record) != 3
                or not isinstance(record[0], str)
                or not (record[1] is None or isinstance(record[1], str))
                or type(record[2]) is not int):
            raise ValueError("record")
        return [record[0], record[1], 1, record[2]]

    def key(self, cell):
        """Sortable key: group, then None devices before string devices."""
        return cell[0], cell[1] is not None, cell[1] or ""

    def combine(self, left, right):
        """Merge two same-key cells into a new cell."""
        return [left[0], left[1], left[2] + right[2], left[3] + right[3]]

    def finish(self, cells):
        """Lazily reduce key-merged cells into public output tuples."""
        group, count, amount, devices = None, 0, 0, 0
        for cell in cells:
            if group is not None and group != cell[0]:
                yield (group, count, amount, devices)
                count, amount, devices = 0, 0, 0
            group = cell[0]
            count += cell[2]
            amount += cell[3]
            devices += cell[1] is not None
        if group is not None:
            yield (group, count, amount, devices)
