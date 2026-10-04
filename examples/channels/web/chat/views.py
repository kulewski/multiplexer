"""The two pages: the room chooser, and the room with its socket."""

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


def index(request: HttpRequest) -> HttpResponse:
    """Ask for a room name and go there."""
    return render(request, "chat/index.html")


def room(request: HttpRequest, room_name: str) -> HttpResponse:
    """The room: a log, an input, and a WebSocket to the consumer."""
    return render(request, "chat/room.html", {"room_name": room_name})
