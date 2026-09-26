"""The views: nothing here knows about replicas or multiplexers, only
Django's cache API, which is the point of a cache backend."""

import os
import time
from datetime import datetime

from django.core.cache import cache
from django.http import HttpRequest, HttpResponse
from django.views.decorators.cache import cache_page


def index(request: HttpRequest) -> HttpResponse:
    """What there is."""
    return HttpResponse(
        "/slow: half a second of work, cached for a minute\n/counter: hits, counted in the cache\n/clear\n"
    )


@cache_page(60)
def slow(request: HttpRequest) -> HttpResponse:
    """Half a second of work; the second request, from any web process, is the cached page."""
    time.sleep(0.5)
    return HttpResponse(f"computed at {datetime.now():%H:%M:%S.%f} by pid {os.getpid()}\n")


def counter(request: HttpRequest) -> HttpResponse:
    """One more hit. `incr` is Django's default, a read then a write, so two web processes at once may
    count one; and a read that misses, a replica started again with nothing, counts from one again."""
    cache.add("hits", 0)
    try:
        hits = cache.incr("hits")
    except ValueError:  # the key was not there when incr read it
        cache.set("hits", 1)
        hits = 1
    return HttpResponse(f"{hits}\n")


def clear(request: HttpRequest) -> HttpResponse:
    """Everything gone, on every replica."""
    cache.clear()
    return HttpResponse("cleared\n")
