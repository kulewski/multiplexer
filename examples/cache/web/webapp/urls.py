"""Four URLs: what there is, the slow page that the cache makes fast, a
counter kept in the cache, and the clear that invalidates everything."""

from django.urls import path

from webapp import views

urlpatterns = [
    path("", views.index),
    path("slow", views.slow),
    path("counter", views.counter),
    path("clear", views.clear),
]
