"""Run from a Python that has the wheel installed and MXCONTROL pointing at
a built multiplexer: a Cluster, a FakePeer answering and a TestClient
asking, through the installed package alone."""

from multiplexer.multiplexer_constants import peers, types
from multiplexer.testing import Cluster, FakePeer, TestClient

with (
    Cluster(1) as cluster,
    FakePeer(cluster, peers.PYTHON_TEST_SERVER) as backend,
    TestClient(cluster, peers.WEBSITE) as client,
):
    backend.reply_with(types.PYTHON_TEST_REQUEST, b"from the wheel", types.PYTHON_TEST_RESPONSE)
    reply = client.query(b"hello", types.PYTHON_TEST_REQUEST)
    assert reply.message == b"from the wheel", reply
    print("wheel smoke: ok, multiplexer", cluster.mx[0].address)
