"""The smallest Django settings that serve the example: no database, no
app, and the cache from the replicas behind the multiplexers named in the
environment."""

import os

SECRET_KEY = "an example; not a secret"
DEBUG = True
ALLOWED_HOSTS = ["*"]
ROOT_URLCONF = "webapp.urls"
INSTALLED_APPS: list[str] = []
MIDDLEWARE = ["django.middleware.common.CommonMiddleware"]
TEMPLATES: list[dict] = []
DATABASES: dict[str, dict] = {}
USE_TZ = True

# The whole of it: every web process shares the replicas' cache.
CACHES = {
    "default": {
        "BACKEND": "mxcache.backend.MultiplexerCache",
        "LOCATION": os.environ.get("MX_ADDRESSES", "127.0.0.1:1980"),
        "TIMEOUT": 300,
    }
}
