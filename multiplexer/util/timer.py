"""Wall-clock timers for ad hoc measurements: Timer as a context manager or
by hand, @autotime to print a function's time, @logtime to log it at
DEBUG/HIGHVERBOSITY. Nothing in the library's own paths uses these."""

import time
from functools import wraps
from typing import Callable

from multiplexer.mxlog import *
from multiplexer.util.decorators import parametrizable_decorator


def _function_pretty_name(f: Callable, cls=None) -> str:
    """module.function or cls.function, for timer names."""
    nametokens = [f.__name__]
    if cls:
        nametokens.append(str(cls))
    else:
        if (
            getattr(f, "__module__", False)
            and getattr(f.__module__, "__name__", False)
            and f.__module__.__name__ != "__main__"
        ):
            nametokens.append(f.__module__.__name__)
    nametokens.reverse()
    return ".".join(nametokens)


@parametrizable_decorator
def autotime(f: Callable, cls=None, silent: bool = False) -> Callable:
    """Decorator: print how long each call of `f` took."""
    timernamepattern = "Timer for %s() #%%d" % _function_pretty_name(f, cls)
    callcounter = [0]

    @wraps(f)
    def wrapper(*args, **kwargs):
        """f, timed and printed."""
        timer = Timer(name=timernamepattern % callcounter[0], silent=silent)
        try:
            callcounter[0] += 1
            ret = f(*args, **kwargs)
        except:
            timer.report(after="exception thrown")
            raise
        else:
            timer.report(after="finish")
            pass
        return ret

    return wrapper


@parametrizable_decorator
def logtime(f: Callable, cls=None) -> Callable:
    """Decorator: log how long each call of `f` took, at DEBUG/HIGHVERBOSITY."""
    functionname = _function_pretty_name(f, cls)
    callcounter = [0]

    @wraps(f)
    def wrapper(*args, **kwargs):
        """f, timed and logged."""
        timer = Timer(silent=True)
        r = f(*args, **kwargs)
        try:
            callcounter[0] += 1
            span = timer.fromstart()
            log(
                DEBUG,
                HIGHVERBOSITY,
                text=lambda: "Call #%d to %s took %.2f s" % (callcounter[0], functionname, span),
            )
        except Exception:
            log_exception()
        return r

    return wrapper


class Timer(object):
    """A stopwatch: restart(), timing(), report(); also a context manager."""

    id = 0
    name = None
    start = None
    last = None

    def __init__(self, name=None, silent=False):
        self.id = Timer.id
        Timer.id += 1
        if name is None:
            self.name = "Timer #" + str(self.id)
        else:
            self.name = name

        self.restart()
        if not silent:
            print("%s: started @ %.3f" % (self.name, self.start))

    def restart(self) -> None:
        """Set both the start and the last mark to now."""
        self.last = self.start = time.time()

    start = restart

    def timing(self) -> tuple[float, float]:
        """(seconds since start, seconds since the last call); moves the last mark."""
        now = time.time()
        try:
            return (now - self.start, now - self.last)
        finally:
            self.last = now

    def fromstart(self) -> float:
        """Seconds since start."""
        return self.timing()[0]

    def fromlast(self) -> float:
        """Seconds since the last call."""
        return self.timing()[1]

    def report(self, after: str | None = None) -> None:
        """Print the timing, optionally labelled with what just finished."""
        times = self.timing()
        print("%s: %s%.3f (%.3f)" % (self.name, after and "after " + after + ": " or "", times[0], times[1]))

    def __enter__(self):
        self.restart()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.report(after="finish")
        self.restart()

    autotime = staticmethod(autotime)
