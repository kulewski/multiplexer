"""Run from a Python that has the wheel installed and MXCONTROL pointing at
a built multiplexer, with the rules file as the argument: Client, the
synchronous client's name up to 2.3.1, is the class SyncClient; and a
Cluster, a FakePeer answering and a TestClient asking work through the
installed package alone. It runs from an empty directory, so that nothing
of the source tree can stand in for what the wheel must carry."""

import os
import sys
import tempfile

RULES = os.path.abspath(sys.argv[1])
os.chdir(tempfile.mkdtemp())

from multiplexer.clients import Client, SyncClient
from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer, TestClient

# The synchronous client under both names is one class.
assert Client is SyncClient, (Client, SyncClient)

with (
    Cluster(1, rules=RULES) as cluster,
    FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend,
    TestClient(cluster, peers.WEBSITE) as client,
):
    backend.reply_with(types.PYTHON_TEST_REQUEST, b"from the wheel", types.PYTHON_TEST_RESPONSE)
    reply = client.query(b"hello", types.PYTHON_TEST_REQUEST)
    assert reply.message == b"from the wheel", reply
    print("wheel smoke: ok, multiplexer", cluster.mx[0].address)
