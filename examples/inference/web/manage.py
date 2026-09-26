#!/usr/bin/env python3
"""Django's command line for the example's web app: `python manage.py runserver`."""

import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webapp.settings")
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))  # the example's modules
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
