#!/usr/bin/env python
"""Django's command line for the chat: `python manage.py runserver`, which daphne serves."""

import os
import sys

# The example's directory, so that mxchannels, channels_pb2 and the constants import.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webapp.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
