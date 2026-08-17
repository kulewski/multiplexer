# Recipe: change a timeout or a limit

Every timeout and limit is a constant in
[multiplexer/defaults.h](../../multiplexer/defaults.h), listed with its
meaning in [guarantees.md](../guarantees.md).

1. **Decide who has to agree.** The heartbeat constants
   (`HEARTBIT_INTERVAL`, the two `NO_HEARTBIT_*` intervals) are a contract
   between a multiplexer and its peers: a peer that heartbeats less often
   than the multiplexer expects is dropped. Change them together and roll
   out multiplexers and peers together. `MAX_MESSAGE_SIZE` is a contract too:
   a receiver with the smaller limit closes the connection. The timeouts and
   queue sizes are local to the process that embeds them.
2. **Edit the constant.** The Python library reads the same values through
   `multiplexer._native`, so there is one place.
3. **Rebuild everything** that embeds it: the multiplexer, every C++ peer,
   and the Python extension.
4. **Update the numbers in the docs**: [guarantees.md](../guarantees.md),
   and the handshake page's frames in `docs/diagrams/generate.py` if a
   heartbeat interval changed; `./format.sh` regenerates the page.
5. **Run the slow tests** as well, `bazel test //...`: `idle_client` and
   the restart scenarios wait out the real intervals.

A per-peer-type queue size needs no rebuild: it is `queue_size` in the rules
file, applied by the multiplexer when the peer connects.
