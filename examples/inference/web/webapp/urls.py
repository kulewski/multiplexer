"""Two URLs: the page with the upload form, and the JSON endpoint curl uses."""

from django.urls import path

from classify import views

urlpatterns = [
    path("", views.index, name="index"),
    path("classify", views.classify, name="classify"),
]
