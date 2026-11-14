"""Generic cleanup helpers shared by scheduler, driver and pipeline."""


def collect(actions):
    """Run each non-None action in order; return BaseExceptions in order."""
    errors = []
    for action in actions:
        if action is not None:
            try:
                action()
            except BaseException as error:
                errors.append(error)
    return errors


def clean(actions, failed):
    """collect, then re-raise the first error unless one is already in flight."""
    errors = collect(actions)
    if errors and not failed:
        raise errors[0]
