"""Two pages: the room chooser, and a room."""

from django.urls import path, re_path

from room import views

urlpatterns = [
    path("", views.index),
    re_path(r"^(?P<room_name>[-a-zA-Z0-9_]{1,90})/$", views.room),  # the names the socket's route takes
]
