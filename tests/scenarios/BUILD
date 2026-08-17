# One mx_integration_test target per scenario and configuration; a scenario
# runs once per combination of role languages it is listed with.
load("//tests:defs.bzl", "mx_integration_test")

# Role-less scenarios: the target name must differ from the folder's.
mx_integration_test(
    name = "server_lifecycle_test",
    scenario = "server_lifecycle/server_lifecycle.py",
)

mx_integration_test(
    name = "direct_queue_full_test",
    size = "medium",
    extra_deps = ["//multiplexer:multiplexer_py_proto"],
    scenario = "direct_queue_full/direct_queue_full.py",
)

LANGS = [
    "py",
    "cc",
]

[
    mx_integration_test(
        name = "query_one_%s_%s" % (backend, client),
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "query_one/query_one.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "backend_raises_%s_%s" % (backend, client),
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "backend_raises/backend_raises.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "round_robin_%s_%s" % (backend, client),
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "round_robin/round_robin.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "concurrent_clients_%s_%s" % (backend, client),
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "concurrent_clients/concurrent_clients.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "two_request_types_%s_%s_%s" % (a, b, client),
        roles = {
            "backend_a": a,
            "backend_b": b,
            "client": client,
        },
        scenario = "two_request_types/two_request_types.py",
    )
    for (a, b, client) in [
        ("py", "py", "py"),
        ("cc", "cc", "cc"),
        ("py", "cc", "py"),
        ("cc", "py", "cc"),
    ]
]

[
    mx_integration_test(
        name = "event_all_backends_%s_%s" % (event_backend, event_client),
        roles = {
            "event_backend": event_backend,
            "event_client": event_client,
        },
        scenario = "event_all_backends/event_all_backends.py",
    )
    for event_backend in LANGS
    for event_client in LANGS
]

[
    mx_integration_test(
        name = "any_vs_all_%s_%s" % (event_backend, event_client),
        roles = {
            "event_backend": event_backend,
            "event_client": event_client,
        },
        scenario = "any_vs_all/any_vs_all.py",
    )
    for event_backend in LANGS
    for event_client in LANGS
]

# Multi-mx and failure scenarios.
[
    mx_integration_test(
        name = "two_mx_backend_on_one_%s_%s" % (backend, client),
        mx = 2,
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "two_mx_backend_on_one/two_mx_backend_on_one.py",
    )
    for (backend, client) in [
        ("py", "py"),
        ("cc", "cc"),
        ("py", "cc"),
    ]
]

[
    mx_integration_test(
        name = "two_mx_backends_on_each_%s" % lang,
        mx = 2,
        roles = {
            "backend": lang,
            "client": lang,
        },
        scenario = "two_mx_backends_on_each/two_mx_backends_on_each.py",
    )
    for lang in LANGS
]

[
    mx_integration_test(
        name = "backend_dies_%s" % lang,
        size = "medium",
        roles = {
            "backend": lang,
            "client": lang,
        },
        scenario = "backend_dies/backend_dies.py",
        tags = ["slow"],
    )
    for lang in LANGS
]

[
    mx_integration_test(
        name = "mx_restarts_%s" % lang,
        size = "medium",
        roles = {
            "backend": lang,
            "client": lang,
        },
        scenario = "mx_restarts/mx_restarts.py",
        tags = ["slow"],
    )
    for lang in LANGS
]

[
    mx_integration_test(
        name = "direct_addressing_%s" % lang,
        roles = {
            "event_backend": lang,
            "event_client": lang,
        },
        scenario = "direct_addressing/direct_addressing.py",
    )
    for lang in LANGS
]

mx_integration_test(
    name = "raw_protocol_py",
    roles = {"event_backend": "py"},
    scenario = "raw_protocol/raw_protocol.py",
)

# Regression tests for the crashes and the handshake bypass found in review.
mx_integration_test(
    name = "hostile_input_py",
    roles = {"event_backend": "py"},
    scenario = "hostile_input/hostile_input.py",
)

[
    mx_integration_test(
        name = "large_payload_%s" % lang,
        size = "medium",
        roles = {
            "backend": lang,
            "client": lang,
        },
        scenario = "large_payload/large_payload.py",
    )
    for lang in LANGS
]

[
    mx_integration_test(
        name = "unrouted_type_%s" % lang,
        roles = {"client": lang},
        scenario = "unrouted_type/unrouted_type.py",
    )
    for lang in LANGS
]

# Characterization of the heartbeat behaviour; idle_client waits out both drop intervals.
[
    mx_integration_test(
        name = "idle_client_%s" % lang,
        size = "medium",
        roles = {
            "backend": lang,
            "client": lang,
        },
        scenario = "idle_client/idle_client.py",
        tags = ["slow"],
    )
    for lang in LANGS
]

[
    mx_integration_test(
        name = "mx_restarts_under_idle_client_%s" % lang,
        size = "medium",
        roles = {
            "backend": lang,
            "client": lang,
        },
        scenario = "mx_restarts_under_idle_client/mx_restarts_under_idle_client.py",
        tags = ["slow"],
    )
    for lang in LANGS
]

# ThreadedClient: an io thread of its own, blocking and callback queries.
[
    mx_integration_test(
        name = "threaded_queries_%s_%s" % (backend, client),
        mx = 2,
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "threaded_queries/threaded_queries.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "threaded_backend_dies_%s" % lang,
        size = "medium",
        roles = {
            "backend": "py",
            "client": lang,
        },
        scenario = "threaded_backend_dies/threaded_backend_dies.py",
    )
    for lang in LANGS
]

[
    mx_integration_test(
        name = "threaded_idle_%s" % lang,
        size = "large",
        roles = {
            "backend": "py",
            "client": lang,
        },
        scenario = "threaded_idle/threaded_idle.py",
        tags = ["slow"],
    )
    for lang in LANGS
]

[
    mx_integration_test(
        name = "threaded_workers_%s_%s" % (backend, client),
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "threaded_workers/threaded_workers.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "mx_restarts_under_events_%s_%s" % (event_backend, event_client),
        size = "medium",
        roles = {
            "event_backend": event_backend,
            "event_client": event_client,
        },
        scenario = "mx_restarts_under_events/mx_restarts_under_events.py",
    )
    for event_backend in LANGS
    for event_client in LANGS
]

mx_integration_test(
    name = "connected_peers_py",
    mx = 2,
    roles = {"backend": "py"},
    scenario = "connected_peers/connected_peers.py",
)

[
    mx_integration_test(
        name = "recording_%s_%s" % (backend, client),
        extra_deps = [
            "//multiplexer:recording",
            "//multiplexer:recording_py_proto",
        ],
        roles = {
            "backend": backend,
            "client": client,
            "event_backend": backend,
            "event_client": client,
        },
        scenario = "recording/recording.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "recording_latency_%s_%s" % (backend, client),
        size = "medium",
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "recording_latency/recording_latency.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "idle_cpu_%s_%s" % (backend, client),
        size = "medium",
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "idle_cpu/idle_cpu.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "backend_drains_until_done_%s_%s" % (backend, client),
        size = "medium",
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "backend_drains_until_done/backend_drains_until_done.py",
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "backend_drains_%s_%s" % (backend, client),
        size = "medium",
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "backend_drains/backend_drains.py",
    )
    for backend in LANGS
    for client in LANGS
]

# A multiplexer frozen with SIGSTOP; slow (waits out the heartbeat drop).
[
    mx_integration_test(
        name = "mx_hangs_%s" % lang,
        size = "large",
        mx = 2,
        roles = {
            "backend": "py",
            "client": lang,
        },
        scenario = "mx_hangs/mx_hangs.py",
        tags = ["slow"],
    )
    for lang in LANGS
]

# A rolling restart of three multiplexers under traffic; slow.
[
    mx_integration_test(
        name = "rolling_restart_%s_%s" % (backend, client),
        size = "large",
        mx = 3,
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "rolling_restart/rolling_restart.py",
        tags = ["slow"],
    )
    for backend in LANGS
    for client in LANGS
]

# Memory: exact heap numbers over long runs; slow, and also the target of check.sh --leaks.
[
    mx_integration_test(
        name = "soak_memory_%s_%s" % (backend, client),
        size = "large",
        roles = {
            "backend": backend,
            "client": client,
        },
        scenario = "soak_memory/soak_memory.py",
        tags = ["slow"],
    )
    for backend in LANGS
    for client in LANGS
]

[
    mx_integration_test(
        name = "threaded_mx_restarts_%s" % lang,
        size = "medium",
        roles = {
            "backend": "py",
            "client": lang,
        },
        scenario = "threaded_mx_restarts/threaded_mx_restarts.py",
    )
    for lang in LANGS
]

# A binary of the scenario's own, named by its label, plays the backend.
py_binary(
    name = "upper_backend",
    srcs = ["label_role/upper_backend.py"],
    main = "label_role/upper_backend.py",
    deps = [
        "//multiplexer:servers",
        "//multiplexer/testing",
        "//tests:testing_constants",
    ],
)

[
    mx_integration_test(
        name = "label_role_%s" % client,
        roles = {
            "backend": ":upper_backend",
            "client": client,
        },
        scenario = "label_role/label_role.py",
    )
    for client in LANGS
]
