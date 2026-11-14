"""Assembly and public lifecycle for the external group aggregate.

Validates options at call time, then wires Rules, Driver and sorter.order.
Owns the input iterator and the final output lifecycle plus the error
priority: a failure already in flight is never replaced by cleanup errors;
on a clean finish the first cleanup error is raised instead. No business
field interpretation, no sorting or merge loops here.
"""
from cleanup import collect
from driver import Driver
from reduction import Rules
from sorter import order


def aggregate(records, workspace, rows_per_run=1000, fanin=8):
    if type(rows_per_run) is not int or rows_per_run < 1:
        raise ValueError("rows_per_run")
    if type(fanin) is not int or fanin < 2:
        raise ValueError("fanin")
    return _execute(records, workspace, rows_per_run, fanin)


def _execute(records, workspace, rows_per_run, fanin):
    driver = Driver(workspace)
    rules = Rules()
    source = None
    cells = None
    failed = False
    try:
        source = iter(records)
        cells = order(source, driver, rules, rows_per_run, fanin)
        yield from rules.finish(cells)
    except BaseException:
        failed = True
        raise
    finally:
        errors = collect([getattr(source, "close", None),
                          getattr(cells, "close", None)])
        errors.extend(driver.cleanup())
        if errors and not failed:
            raise errors[0]
