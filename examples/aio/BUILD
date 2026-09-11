# The aio example: an asyncio TCP gateway in front of a chat backend.
exports_files(["chat.rules"])

py_binary(
    name = "backend",
    srcs = ["backend.py"],
    deps = [
        "@mx//multiplexer:multiplexer_constants",
        "@mx//multiplexer:servers",
    ],
)

py_binary(
    name = "gateway",
    srcs = ["gateway.py"],
    deps = [
        "@mx//multiplexer:aio",
        "@mx//multiplexer:multiplexer_constants",
    ],
)

py_test(
    name = "gateway_test",
    size = "medium",
    srcs = [
        "backend.py",
        "gateway.py",
        "gateway_test.py",
    ],
    deps = [
        "@mx//multiplexer:aio",
        "@mx//multiplexer:multiplexer_constants",
        "@mx//multiplexer:servers",
        "@mx//multiplexer/testing",
    ],
)
