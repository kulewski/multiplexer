#!/usr/bin/env python3
"""Draws docs/kafka.md's charts and writes its result tables from the
runs of the Kafka comparison kept in tests/kafka/results.jsonl
(`RESULTS=tests/kafka/results.jsonl tests/kafka/run.sh`): an SVG chart
for each question, one for GitHub's light theme and one for its dark,
into docs/assets/kafka/, and the tables between the page's
`<!-- kafka_charts.py: NAME -->` and `<!-- kafka_charts.py: end -->`
lines. So the charts, the tables and the runs cannot disagree: run
./format.sh after a run, and ./format.sh --check fails when the page or a
chart is stale.

Each figure is the median of a configuration's runs. A chart is
horizontal bars on a linear axis, the multiplexer blue and Kafka gray,
each bar's value at its end, latency as its median and its 99th
percentile, the tables giving the rest; written by hand, as SVG, with
nothing but the standard library.

Usage: kafka_charts.py [--check]
"""

import json
import math
import os
import statistics
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "tests", "kafka", "results.jsonl")
ASSETS = os.path.join(ROOT, "docs", "assets", "kafka")
PAGE = os.path.join(ROOT, "docs", "kafka.md")

# What every chart says it measured, under its title.
SETUP = "the multiplexer 4.0.0 · Kafka 4.3.1, librdkafka 2.8.0 · AMD Ryzen Threadripper PRO 9965WX"
NAMES = {"mx": "multiplexer", "kafka": "Kafka"}
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif"
# GitHub's own colors for the page, text and lines, light and dark; the
# multiplexer blue and Kafka gray in both, as simdjson's charts have them.
# The page's color is the chart's background, so that a viewer that shows
# the light chart on a dark page still shows it whole.
THEMES = {
    "light": {
        "page": "#ffffff",
        "text": "#1f2328",
        "muted": "#59636e",
        "grid": "#d1d9e0",
        "mx": "#2a78d6",
        "kafka": "#c4c3bd",
    },
    "dark": {
        "page": "#0d1117",
        "text": "#f0f6fc",
        "muted": "#9198a1",
        "grid": "#3d444d",
        "mx": "#3987e5",
        "kafka": "#4a4f58",
    },
}
WIDTH = 960
LABELS = 210  # the column of bar labels, right-aligned at its edge
VALUES = 110  # room after the longest bar for its value
TOP = 150  # the plot's top, under the title, subtitle and setup lines
BAR = 26  # a bar's height
STEP = 40  # from one bar to the next in a group
GAP = 22  # more between groups


def load(path):
    """Every run, by configuration: (flavour, producers, consumers, size,
    rate, system) to a list of runs."""
    groups = {}
    with open(path) as lines:
        for line in lines:
            run = json.loads(line)
            key = (run["flavour"], run["producers"], run["threads"], run["size"], run["rate"], run["system"])
            groups.setdefault(key, []).append(run)
    return groups


def median(runs, value):
    return statistics.median(value(run) for run in runs)


def millions(value):
    return "%.2f M" % (value / 1e6)


def microseconds(value):
    """A latency, given in µs, as the page writes it: µs below a
    millisecond, ms below a second, seconds above, a whole number of them
    as such."""
    if value >= 1e6:
        return ("%.1f s" % (value / 1e6)).replace(".0 s", " s")
    if value >= 1000:
        return ("%.1f ms" % (value / 1000)).replace(".0 ms", " ms")
    return "%d µs" % round(value)


def number(value):
    return format(round(value), ",")


def configuration(producers, consumers):
    return "%d → %d" % (producers, consumers)


def spelled(producers, consumers):
    """A configuration as a chart labels a bar."""
    return "%d producer%s, %d consumer%s" % (
        producers,
        "" if producers == 1 else "s",
        consumers,
        "" if consumers == 1 else "s",
    )


class Chart:
    """A chart: groups of bars, each bar (system, label under it, value,
    the value as written), on a linear axis from 0 to `top` with `ticks`."""

    def __init__(self, name, title, subtitle, axis, groups, top, ticks, tick_text=str):
        self.name, self.title, self.subtitle, self.axis = name, title, subtitle, axis
        self.groups, self.top, self.ticks, self.tick_text = groups, top, ticks, tick_text

    def fraction(self, value):
        """How far along the axis `value` is, 0 to 1."""
        return min(1.0, max(0.0, value / self.top))

    def description(self):
        """Every value, for a reader without the picture."""
        parts = []
        for group in self.groups:
            parts.append("; ".join("%s, %s: %s" % (NAMES[system], under, text) for system, under, _, text in group))
        return ". ".join(parts) + "."

    def svg(self, theme):
        colors = THEMES[theme]
        plot_left, plot_right = LABELS, WIDTH - VALUES
        span = plot_right - plot_left
        rows = []
        y = TOP
        for group in self.groups:
            for bar in group:
                rows.append((y, bar))
                y += STEP
            y += GAP
        bottom = y - GAP - STEP + BAR + 12
        height = bottom + 70
        out = [
            '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" role="img"'
            ' aria-labelledby="title description" font-family="%s">' % (WIDTH, height, WIDTH, height, FONT),
            '<title id="title">%s</title>' % escape(self.title),
            '<desc id="description">%s</desc>' % escape(self.description()),
            '<rect width="%d" height="%d" rx="6" fill="%s"/>' % (WIDTH, height, colors["page"]),
            '<text x="24" y="44" font-size="24" font-weight="600" fill="%s">%s</text>'
            % (colors["text"], escape(self.title)),
            '<text x="24" y="78" font-size="17" fill="%s">%s</text>' % (colors["muted"], escape(self.subtitle)),
            '<text x="24" y="106" font-size="14" fill="%s">%s</text>' % (colors["muted"], escape(SETUP)),
        ]
        for tick in self.ticks:
            x = plot_left + span * self.fraction(tick)
            out.append(
                '<line x1="%.1f" y1="%d" x2="%.1f" y2="%d" stroke="%s" stroke-width="1"/>'
                % (x, TOP - 14, x, bottom, colors["grid"])
            )
            out.append(
                '<text x="%.1f" y="%d" font-size="14" text-anchor="middle" fill="%s">%s</text>'
                % (x, bottom + 24, colors["muted"], escape(self.tick_text(tick)))
            )
        out.append(
            '<text x="%.1f" y="%d" font-size="15" text-anchor="middle" fill="%s">%s</text>'
            % (plot_left + span / 2, bottom + 56, colors["muted"], escape(self.axis))
        )
        for y, (system, under, value, text) in rows:
            length = max(span * self.fraction(value), 2.0)
            out.append(
                '<rect x="%d" y="%d" width="%.1f" height="%d" rx="3" fill="%s"/>'
                % (plot_left, y, length, BAR, colors[system])
            )
            out.append(
                '<text x="%d" y="%d" font-size="17" text-anchor="end" fill="%s">%s</text>'
                % (plot_left - 14, y + 12, colors["text"], escape(NAMES[system]))
            )
            out.append(
                '<text x="%d" y="%d" font-size="13" text-anchor="end" fill="%s">%s</text>'
                % (plot_left - 14, y + 28, colors["muted"], escape(under))
            )
            out.append(
                '<text x="%.1f" y="%d" font-size="16" fill="%s">%s</text>'
                % (plot_left + length + 8, y + 19, colors["text"], escape(text))
            )
        out.append("</svg>")
        return "\n".join(out) + "\n"


def escape(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def nice_top(value):
    """The axis's end, the first of 1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8 or
    10 times a power of ten at or above `value`, and the step of its
    ticks, four to six of them."""
    power = 10 ** math.floor(math.log10(value))
    for top, step in (
        (1, 0.2),
        (1.2, 0.2),
        (1.5, 0.25),
        (2, 0.4),
        (2.5, 0.5),
        (3, 0.5),
        (4, 1),
        (5, 1),
        (6, 1),
        (8, 2),
        (10, 2),
    ):
        if value <= top * power:
            return top * power, step * power
    return 10 * power, 2 * power


def charts(groups):
    """The page's charts: records a second batched, one chart for each
    record size on one scale; how late a record arrives not batched, one
    chart for each rate."""
    found = []
    fastest = {key: runs for key, runs in groups.items() if key[0] == "throughput" and not key[4]}
    sizes = sorted({key[3] for key in fastest})
    top, step = nice_top(max(median(runs, lambda run: run["records_per_second"]) for runs in fastest.values()))
    ticks = [step * index for index in range(int(round(top / step)) + 1)]
    for size in sizes:
        configurations = sorted({(key[1], key[2]) for key in fastest if key[3] == size})
        chart_groups = []
        for producers, consumers in configurations:
            bars = []
            for system in ("mx", "kafka"):
                runs = fastest.get(("throughput", producers, consumers, size, 0, system))
                if runs:
                    rate = median(runs, lambda run: run["records_per_second"])
                    bars.append((system, spelled(producers, consumers), rate, millions(rate) + "/s"))
            chart_groups.append(bars)
        found.append(
            Chart(
                "throughput-%d" % size,
                "%s-byte records, both batching" % number(size),
                "records a second, as fast as each goes",
                "million records a second (higher is better)",
                chart_groups,
                top=top,
                ticks=ticks,
                tick_text=lambda tick: "%g" % (tick / 1e6),
            )
        )
    steady = {key: runs for key, runs in groups.items() if key[0] == "latency" and key[4]}
    for rate in sorted({key[4] * key[1] for key in steady}):
        chart_groups = []
        for percentile, name in (("p50", "p50"), ("p99", "p99")):
            bars = []
            for system in ("mx", "kafka"):
                runs = [r for key, r in steady.items() if key[4] * key[1] == rate and key[5] == system]
                if runs:
                    value = median(runs[0], lambda run, percentile=percentile: run["latency_us"][percentile])
                    bars.append((system, name, value, microseconds(value)))
            chart_groups.append(bars)
        top, step = nice_top(max(bar[2] for bars in chart_groups for bar in bars))
        found.append(
            Chart(
                "latency-%d" % rate,
                "Not batching, %s records a second" % number(rate),
                "how late a record arrives, from its send to its arrival",
                "latency (lower is better)",
                chart_groups,
                top=top,
                ticks=[step * index for index in range(int(round(top / step)) + 1)],
                tick_text=tick_time,
            )
        )
    return found


def tick_time(value):
    """An axis tick's time, given in µs, in µs below a millisecond and in
    ms from one, as precise as the tick is."""
    if value >= 1000:
        return "%g ms" % (value / 1000)
    return "%g µs" % value


def tables(groups):
    """The page's tables, by the name of their place in it."""
    found = {}
    rows = [
        "| Producers → consumers | Record | System | Records/s | Range over runs | Sent/s | p50 latency | Broker CPU s a million records | Lost |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    fastest = sorted(
        (key for key in groups if key[0] == "throughput" and not key[4]),
        key=lambda key: (key[3], key[1], key[2], key[5] != "mx"),
    )
    for key in fastest:
        runs = groups[key]
        rates = [run["records_per_second"] for run in runs]
        rows.append(
            "| %s | %s B | %s | %s | %s–%s | %s | %s | %.2f | %s |"
            % (
                configuration(key[1], key[2]),
                number(key[3]),
                NAMES[key[5]],
                number(statistics.median(rates)),
                number(min(rates)),
                number(max(rates)),
                number(median(runs, lambda run: run["sent_per_second"])),
                microseconds(median(runs, lambda run: run["latency_us"]["p50"])),
                median(runs, lambda run: run["broker_cpu_per_million"]),
                number(median(runs, lambda run: run["expected"] - run["received"])),
            )
        )
    found["throughput"] = "\n".join(rows)
    for flavour, name in (("latency", "latency-not-batched"), ("throughput", "latency-batched")):
        rows = [
            "| Rate | Record | System | p50 | p99 | p99.9 | Max | Lost |",
            "|---|---|---|---|---|---|---|---|",
        ]
        steady = sorted(
            (key for key in groups if key[0] == flavour and key[4]),
            key=lambda key: (key[4] * key[1], key[3], key[5] != "mx"),
        )
        for key in steady:
            runs = groups[key]
            rows.append(
                "| %s/s | %s B | %s | %s | %s | %s | %s | %s |"
                % (
                    number(key[4] * key[1]),
                    number(key[3]),
                    NAMES[key[5]],
                    *(
                        microseconds(median(runs, lambda run, p=p: run["latency_us"][p]))
                        for p in ("p50", "p99", "p999", "max")
                    ),
                    number(median(runs, lambda run: run["expected"] - run["received"])),
                )
            )
        found[name] = "\n".join(rows)
    return found


def picture(chart):
    """The page's `<picture>` for a chart, its dark version in GitHub's
    dark theme, every value in its alt text."""
    return (
        "<picture>\n"
        '  <source media="(prefers-color-scheme: dark)" srcset="assets/kafka/%s-dark.svg">\n'
        '  <img alt="%s: %s" src="assets/kafka/%s.svg">\n'
        "</picture>" % (chart.name, escape(chart.title), escape(chart.description()).replace('"', "&quot;"), chart.name)
    )


def page(text, blocks):
    """`text` with each `<!-- kafka_charts.py: NAME -->` block's content
    replaced by `blocks[NAME]`."""
    out = []
    lines = text.split("\n")
    index = 0
    while index < len(lines):
        line = lines[index]
        out.append(line)
        if line.startswith("<!-- kafka_charts.py: ") and line != "<!-- kafka_charts.py: end -->":
            name = line[len("<!-- kafka_charts.py: ") : -len(" -->")]
            if name not in blocks:
                sys.exit("kafka_charts.py: %s names no table or chart" % name)
            end = lines.index("<!-- kafka_charts.py: end -->", index + 1)
            out.append(blocks[name])
            out.append(lines[end])
            index = end + 1
            continue
        index += 1
    return "\n".join(out)


def main():
    check = sys.argv[1:] == ["--check"]
    groups = load(RESULTS)
    files = {}
    blocks = tables(groups)
    for chart in charts(groups):
        for theme, suffix in (("light", ""), ("dark", "-dark")):
            files[os.path.join(ASSETS, chart.name + suffix + ".svg")] = chart.svg(theme)
        blocks["chart " + chart.name] = picture(chart)
    with open(PAGE) as current:
        files[PAGE] = page(current.read(), blocks)
    stale = []
    for path, text in files.items():
        old = open(path).read() if os.path.exists(path) else None
        if old != text:
            stale.append(os.path.relpath(path, ROOT))
            if not check:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as out:
                    out.write(text)
    if check and stale:
        print("kafka_charts.py: stale, run ./format.sh: " + ", ".join(stale), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
