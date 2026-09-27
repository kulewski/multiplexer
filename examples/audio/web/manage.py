#!/usr/bin/env python
"""Django's command line for the gateway: `python manage.py runserver`, which daphne serves."""

import os
import sys

# The example's directory first, for its constants and payloads; then the
# channels example's, for the channel layer this gateway runs on.
EXAMPLE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(EXAMPLE), "channels"))
sys.path.insert(0, EXAMPLE)

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webapp.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
