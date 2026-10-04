"""The WebSocket route: one consumer per socket, in a room."""

from typing import Any, Callable, cast

from django.urls import re_path

from room import consumers

# The cast and the annotation: Django's stubs type a route's target as a
# view, and a consumer's ASGI application is not one to them.
websocket_urlpatterns: list[Any] = [
    # Room names Channels' group names can hold: ASCII, and short enough for "audio." in front.
    re_path(
        r"ws/audio/(?P<room_name>[-a-zA-Z0-9_]{1,90})/$", cast(Callable[..., Any], consumers.AudioConsumer.as_asgi())
    ),
]
