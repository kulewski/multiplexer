"""The smallest Django settings that serve the room: Channels with the
multiplexers named in the environment as its channel layer, the static
files of the page, and no database."""

import os

SECRET_KEY = "an example; not a secret"
DEBUG = True
ALLOWED_HOSTS = ["*"]
INSTALLED_APPS = ["daphne", "channels", "django.contrib.staticfiles", "room"]
MIDDLEWARE = ["django.middleware.common.CommonMiddleware"]
ROOT_URLCONF = "webapp.urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True}]
DATABASES: dict[str, dict] = {}
USE_TZ = True
STATIC_URL = "static/"
ASGI_APPLICATION = "webapp.asgi.application"

# The channel layer is the multiplexer, from the channels example; the
# same connections carry the frames to the workers. A socket in a room of
# N takes N*100 frames a second, so a capacity of 1000 lets its consumer
# stall for 1000/(N*100) seconds, a second in a room of ten, before the
# layer drops frames for it, counted and logged; Channels' default of 100
# would be a tenth of that.
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "mxchannels.MultiplexerChannelLayer",
        "CONFIG": {"addresses": os.environ.get("MX_ADDRESSES", "127.0.0.1:1980"), "capacity": 1000},
    }
}

# What a stream lost, when its socket closes, and the layer's warnings.
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "loggers": {"room": {"handlers": ["console"], "level": "INFO"}, "mxchannels": {"handlers": ["console"]}},
}
