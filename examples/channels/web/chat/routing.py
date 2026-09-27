"""The WebSocket route: one consumer per room."""

from typing import Any, Callable, cast

from django.urls import re_path

from chat import consumers

# The cast and the annotation: Django's stubs type a route's target as a
# view, and a consumer's ASGI application is not one to them, nor is the
# resulting pattern what Channels' URLRouter is typed to take.
websocket_urlpatterns: list[Any] = [
    re_path(r"ws/chat/(?P<room_name>[\w-]+)/$", cast(Callable[..., Any], consumers.ChatConsumer.as_asgi())),
]
