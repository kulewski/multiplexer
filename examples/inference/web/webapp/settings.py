"""The smallest Django settings that serve the example: one app, no
database, and the multiplexers' addresses from the environment."""

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECRET_KEY = "an example; not a secret"
DEBUG = True
ALLOWED_HOSTS = ["*"]
ROOT_URLCONF = "webapp.urls"
INSTALLED_APPS = ["classify"]
MIDDLEWARE = ["django.middleware.common.CommonMiddleware", "django.middleware.csrf.CsrfViewMiddleware"]
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True}]
DATABASES = {}
USE_TZ = True

# host:port of every multiplexer, comma-separated; every web process
# connects to all of them and fails over between them on its own.
MULTIPLEXER_ADDRESSES = [
    (host, int(port))
    for host, port in (item.rsplit(":", 1) for item in os.environ.get("MX_ADDRESSES", "127.0.0.1:1980").split(","))
]
# The timeout of each stage of a view's query (docs/query.md): an answer
# that does not come starts the search for another worker, so a view gives
# up after at most three times this, twice for a worker that hung.
INFER_TIMEOUT = float(os.environ.get("INFER_TIMEOUT", "10"))
# The largest upload a view passes on; a bigger one gets 413. A message is
# limited to 128 MiB, and an image the model reads is a few kilobytes.
MAX_IMAGE_BYTES = int(os.environ.get("MAX_IMAGE_BYTES", str(4 * 1024 * 1024)))
