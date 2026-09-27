"""The two pages: the room chooser, and the room with its socket, its canvas and its meter."""

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


def index(request: HttpRequest) -> HttpResponse:
    """Ask for a room name and go there."""
    return render(request, "room/index.html")


def room(request: HttpRequest, room_name: str) -> HttpResponse:
    """The room."""
    return render(request, "room/room.html", {"room_name": room_name})
