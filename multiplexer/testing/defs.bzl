"""mx_integration_test: one scenario file, one target per configuration."""

# The roles this repository ships, by the language a scenario names.
ROLE_BINARIES = {
    "py": [
        Label("//tests/roles/py:backend"),
        Label("//tests/roles/py:client"),
        Label("//tests/roles/py:event_client"),
        Label("//tests/roles/py:event_backend"),
    ],
    "cc": [Label("//tests/roles/cc:mxtestroles")],
}

def mx_integration_test(
        name,
        scenario,
        roles = {},
        mx = 1,
        params = {},
        rules = Label("//:multiplexer_rules"),
        size = "small",
        tags = [],
        deps = [],
        data = [],
        **kwargs):
    """Runs `scenario` (a unittest file using multiplexer.testing) as a py_test.

    roles maps a role name to who plays it: "py" or "cc" for the roles this
    repository ships, or the label of a binary of your own that follows the
    role contract (tests/README.md); the scenario sees "bin" for it. mx is
    the number of multiplexers; rules the rules file they run with, by
    default the one the multiplexer_rules flag names, so that it matches
    the generated constants; params is free-form and reaches the scenario
    through CONFIG.param(). deps and data add what the scenario needs
    beyond multiplexer.testing.
    """
    langs = {}
    scenario_roles = {}
    role_binaries = {}
    data = list(data) + [Label("//mxcontrol"), rules]
    for role, player in roles.items():
        if player in ROLE_BINARIES:
            langs[player] = None
            scenario_roles[role] = player
        else:
            langs["bin"] = None
            scenario_roles[role] = "bin"
            role_binaries[role] = "$(rootpath %s)" % player
            data.append(player)
    for lang in langs:
        data = data + ROLE_BINARIES.get(lang, [])
    args = [
        "--mx",
        str(mx),
        "--roles",
        "'%s'" % json.encode(scenario_roles),
        "--rules",
        "$(rootpath %s)" % rules,
        "--params",
        "'%s'" % json.encode(params),
    ]
    if role_binaries:
        args += ["--role-binaries", "'%s'" % json.encode(role_binaries)]
    native.py_test(
        name = name,
        srcs = [scenario],
        main = scenario,
        args = args,
        data = data,
        deps = [Label("//multiplexer/testing")] + deps,
        size = size,
        tags = tags + ["integration"] + ["lang-" + lang for lang in sorted(langs.keys())],
        **kwargs
    )
