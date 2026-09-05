"""Structured logging for Python peers, sharing the C++ library's stream.

log(level, verbosity, text=..., ctx=..., data=..., flow=...) builds a
LogEntry and hands it to the C++ side (multiplexer._native), which writes the
text form to stderr and, if configured, the binary form to a file or fd. The
level and verbosity constants (DEBUG, INFO, ..., LOWVERBOSITY, ...) come from
the C++ side too, so both languages filter the same way.

Cost: log() asks should_log() first and `text` may be a callable, so a
disabled level costs one call and no string formatting.
"""

import sys
import os
import random
import pickle
import traceback
from time import time
from typing import Any, Callable
from multiplexer.util.decorators import (
    parametrizable_decorator,
    never_throw,
)

from multiplexer._native import *
from multiplexer.mxlog.type_id_constants import *
from multiplexer.release import version
import multiplexer._native as _logging


__all__ = ["should_log", "log", "log_call", "log_exception", "PickleData"] + list(
    k for k in dir(_logging) if isinstance(k, str) and k.isupper()
)


@never_throw(default=False)
def should_log(level: int, verbosity: int) -> bool:
    """Whether an entry at `level` with `verbosity` would be emitted."""
    assert isinstance(level, int)
    assert isinstance(verbosity, int)
    return _logging.should_log(level, verbosity)


def log(level: int, verbosity: int, *args, **kwarg) -> None:
    """Emit a log entry if the level is enabled; see _do_log for the keywords.
    Never raises."""
    assert isinstance(level, int)
    assert isinstance(verbosity, int)
    if not should_log(level, verbosity):
        return
    return do_log(level, verbosity, *args, **kwarg)


log_defaults = {"version": version}


@never_throw
def do_log(level: int, verbosity: int, **kwargs) -> None:
    """log() without the level check; adds the module-wide log_defaults."""
    if log_defaults:
        kwargs = dict(log_defaults, **kwargs)
    return _do_log(level, verbosity, **kwargs)


class PickleData(object):
    """A log `data` payload pickled lazily, only if the entry is emitted."""

    __slots__ = ("_pkl", "_obj")

    def __init__(self, obj):
        self._obj = obj
        self._pkl = None

    @property
    def pickle(self):
        """The pickled bytes, computed once; repr() for objects that cannot be pickled."""
        if self._pkl is None:
            try:
                self._pkl = pickle.dumps(self._obj)
            except TypeError:
                # C++-exported classes are (at least sometimes) not picklable
                self._pkl = repr(self._obj)
            assert self._pkl is not None

            self._obj = None
        return self._pkl


def _make_string(s):
    """`s` if it is a str; ValueError otherwise."""
    if isinstance(s, str):
        return s
    raise ValueError


def _do_log(
    level: int,
    verbosity: int,
    ctx: str | None = None,
    context: str | None = None,
    text: str | Callable[[], str] | None = None,
    data: bytes | str | PickleData | Callable[[], Any] | None = None,
    data_type: int | None = None,
    flow: str | None = None,
    flags: int = 0,
    version: str | None = None,
) -> None:
    """Build a LogEntry and hand it to the C++ side. `ctx` is appended to the
    process context, `context` replaces it; `text` and `data` may be
    callables evaluated only now; `data` needs a `data_type` unless it is a
    PickleData; `flow` is the workflow id. Raises on misuse, which the
    public wrappers turn into a printed warning."""
    assert isinstance(level, int)
    assert isinstance(verbosity, int)
    assert ctx is None or context is None
    assert ctx is None or isinstance(ctx, str)
    assert context is None or isinstance(context, str)
    assert text is None or isinstance(text, str) or callable(text)
    # data can be anything for now
    assert data_type is None or isinstance(data_type, int)
    assert flow is None or isinstance(flow, str)

    log_msg = _logging.LogEntry()

    # id, timestamp
    log_msg.id = _logging.create_log_id()
    log_msg.timestamp = int(time())
    if version is not None:
        log_msg.version = version

    # context
    if context is None:
        context = _logging.process_context()
        if ctx is not None:
            context += "." + ctx
    log_msg.context = context

    # level, verbosity
    log_msg.level = level
    log_msg.verbosity = verbosity

    # text
    if callable(text):
        text = text()
    assert text is None or isinstance(text, str)
    if text is not None:
        log_msg.text = _make_string(text)

    # data, data_type
    if callable(data):
        data = data()
    if data is not None and data_type is None:
        if isinstance(data, PickleData):
            data_type = PYTHON_PICKLE
        else:
            raise ValueError("data_type is missing and data is not")

    if isinstance(data, PickleData):
        data = data.pickle
    if isinstance(data, str):
        data = data.encode("utf-8")
    if data is not None and not isinstance(data, bytes):
        raise ValueError("data must be bytes, str or PickleData")
    if data is not None:
        log_msg.data = data
    if data_type is not None:
        log_msg.data_type = data_type

    # data_class ?

    # workflow
    if flow is not None:
        log_msg.workflow = flow

    # pid
    log_msg.pid = os.getpid()

    # TODO(findepi): think if this can be retrieved (stack analysis; see
    #       python-gflags code for example)
    # source_file
    # source_line
    # compilation_datetime

    _logging.emit_log(log_msg, flags)


# Logs entry and exit of `f` with its arguments and result, pickled, at a
# level that is off by default; used on most methods of the client library.
@parametrizable_decorator
def log_call(f: Callable, level: int = DEBUG, verbosity: int = CHATTERBOX) -> Callable:
    """Decorator: log entry and exit of `f` with arguments and result."""
    assert isinstance(level, int)
    assert isinstance(verbosity, int)
    assert callable(f)

    callidmax = sys.maxsize

    def wrapper(*args, **kwargs):
        """f with the two log entries around it, when the level is on."""
        if not should_log(level, verbosity):
            return f(*args, **kwargs)

        logsig = {"fname": f.__name__, "callid": random.randint(1, callidmax)}
        do_log(
            level,
            verbosity,
            text="entering %s(args= %r, kwargs= %r)" % (f.__name__, args, kwargs),
            data=PickleData(dict(logsig, args=args, kwargs=kwargs)),
        )
        try:
            ret = f(*args, **kwargs)
        except Exception as exc:
            do_log(
                level,
                verbosity,
                text="leaving %s after exception %r" % (f.__name__, exc),
                data=PickleData(dict(logsig, exc=exc)),
            )
            raise
        else:
            do_log(
                level,
                verbosity,
                text="leaving %s with %r" % (f.__name__, ret),
                data=PickleData(dict(logsig, ret=ret)),
            )
            return ret

    return wrapper


def log_exception(
    level: int = ERROR, verbosity: int = LOWVERBOSITY, text: str = "Unhandled exception occurred", **kwargs
) -> None:
    """Log the exception being handled, with its traceback printed to stderr."""
    traceback.print_stack()
    traceback.print_exc()
    kwargs = dict({"data": lambda: PickleData(sys.exc_info()[:2])}, **kwargs)
    log(level, verbosity, text=text, **kwargs)
