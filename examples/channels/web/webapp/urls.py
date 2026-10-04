"""Two pages: the room chooser, and a room."""

from django.urls import path

from chat import views

urlpatterns = [
    path("", views.index),
    path("<slug:room_name>/", views.room),
]
