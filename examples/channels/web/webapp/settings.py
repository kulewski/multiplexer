"""The smallest Django settings that serve the chat: Channels, with the
multiplexers named in the environment as its channel layer, and no
database."""

import os

SECRET_KEY = "an example; not a secret"
DEBUG = True
ALLOWED_HOSTS = ["*"]
INSTALLED_APPS = ["daphne", "channels", "chat"]
MIDDLEWARE = ["django.middleware.common.CommonMiddleware"]
ROOT_URLCONF = "webapp.urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True}]
DATABASES: dict[str, dict] = {}
USE_TZ = True
ASGI_APPLICATION = "webapp.asgi.application"

# The whole of it: the channel layer is the multiplexer.
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "mxchannels.MultiplexerChannelLayer",
        "CONFIG": {"addresses": os.environ.get("MX_ADDRESSES", "127.0.0.1:1980")},
    }
}
