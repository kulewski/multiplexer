"""Run from a Python that has the wheel installed and MXCONTROL pointing at
a built multiplexer, with the system rules file as the argument: a rules
file of the smoke's own, the system rules and a client's and a backend's
types after them, then a Cluster on it, a FakePeer answering and a
TestClient asking, through the installed package alone. It runs from an
empty directory, so that nothing of the source tree can stand in for what
the wheel must carry."""

import os
import sys
import tempfile

SYSTEM_RULES = os.path.abspath(sys.argv[1])
os.chdir(tempfile.mkdtemp())

from multiplexer.testing import Cluster, FakePeer, TestClient

# The smoke's own types, after the system rules, as every rules file has
# them; its code names them by number, having no constants of its own.
CLIENT, BACKEND, REQUEST, RESPONSE = 200, 201, 301, 302
with open(SYSTEM_RULES) as system, open("smoke.rules", "w") as rules:
    rules.write(system.read())
    rules.write(
        'peer { type: 200 name: "SMOKE_CLIENT" is_passive: true }\n'
        'peer { type: 201 name: "SMOKE_BACKEND" }\n'
        'type { type: 301 name: "SMOKE_REQUEST" to { peer: "SMOKE_BACKEND" whom: ANY } }\n'
        'type { type: 302 name: "SMOKE_RESPONSE" }\n'
    )

with (
    Cluster(1, rules=os.path.abspath("smoke.rules")) as cluster,
    FakePeer(cluster, BACKEND) as backend,
    TestClient(cluster, CLIENT) as client,
):
    backend.reply_with(REQUEST, b"from the wheel", RESPONSE)
    reply = client.query(b"hello", REQUEST)
    assert reply.message == b"from the wheel", reply
    print("wheel smoke: ok, multiplexer", cluster.mx[0].address)
