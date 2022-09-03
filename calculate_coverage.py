#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys

import pkg_resources
import pytest
from coverage import Coverage

DEFAULT_COLOR = "#a4a61d"
COLORS = {
    "brightgreen": "#4c1",
    "green": "#97CA00",
    "yellowgreen": "#a4a61d",
    "yellow": "#dfb317",
    "orange": "#fe7d37",
    "red": "#e05d44",
    "lightgrey": "#9f9f9f",
}

COLOR_RANGES = [
    (95, "brightgreen"),
    (90, "green"),
    (75, "yellowgreen"),
    (60, "yellow"),
    (40, "orange"),
    (0, "red"),
]


def get_color(total):
    """
    Return color for current coverage precent
    """
    try:
        xtotal = int(total)
    except ValueError:
        return COLORS["lightgrey"]
    for range_, color in COLOR_RANGES:
        if xtotal >= range_:
            return COLORS[color]


def get_badge(total, color=DEFAULT_COLOR):
    """
    Read the SVG template from the package, update total, return SVG as a
    string.
    """
    template_path = "flat.svg"
    template = pkg_resources.resource_string(__name__, template_path).decode("utf8")
    return template.replace("{{ total }}", str(int(total))).replace("{{ color }}", color)


def save_badge(badge, filepath):
    """
    Save badge to the specified path.
    """
    path = os.path.abspath(filepath)

    # Write file
    with open(path, "w", encoding="utf-8") as file:
        file.write(badge)

    return path


def main():
    """Check minimum coverage"""

    cov = Coverage(source=".", omit=["calculate_coverage.py", "setup.py"])
    cov.erase()
    cov.start()
    pytest.main()
    cov.stop()
    cov.save()
    covered = cov.report(show_missing=True)
    percentage = 95
    color = get_color(covered)
    badge = get_badge(covered, color)
    filepath = "coverage.svg"
    path = save_badge(badge, filepath)
    print(f"Saved badge to {path}")

    if covered < percentage:
        print(f"Coverage < {percentage}")
        sys.exit(1)


if __name__ == "__main__":
    main()
