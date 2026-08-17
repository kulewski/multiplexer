"""Two decorators used by the logging module.

parametrizable_decorator lets a decorator be applied both as @d and as
@d(option=...); never_throw swallows any exception from the wrapped
function, printing it, so that logging can never take a program down.
"""

import sys
import traceback
import functools
from typing import Callable


def _update_wrapper(wrapper, wrapped, *args, **kwargs):
    """Run functools.update_wrapper iff `wrapper` is not `wrapped`."""
    if wrapper is not wrapped:
        functools.update_wrapper(wrapper, wrapped, *args, **kwargs)
    return wrapper


def parametrizable_decorator(decorator: Callable) -> Callable:
    """Make `decorator` usable both as @decorator and as @decorator(option=...)."""

    @functools.wraps(decorator)
    def wrapper(fn=None, *args, **kwargs):
        """Apply now if given the function, else return the configured decorator."""
        if fn is not None:
            return _update_wrapper(decorator(fn, *args, **kwargs), fn)
        else:
            return lambda fn: _update_wrapper(decorator(fn, *args, **kwargs), fn)

    return wrapper


@parametrizable_decorator
def never_throw(fn: Callable, default=None) -> Callable:
    """Decorator: run `fn`, print any exception with its stack, return `default`."""

    def wrapper(*args, **kwargs):
        """fn, with exceptions turned into a printed warning."""
        try:
            return fn(*args, **kwargs)
        except Exception:
            traceback.print_stack()
            traceback.print_exc()
            print("Ignored.", file=sys.stderr)
            return default

    return wrapper
