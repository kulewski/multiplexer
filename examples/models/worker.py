"""A model worker: a backend on `BaseMultiplexerServer` that holds some of
a service's models and answers `PREDICT` for them, its key filters naming
the models it holds and the worker itself.

    python worker.py [ADDRESSES] [--name NAME] [--models M1,M2,... | --any-model]

ADDRESSES is host:port of every multiplexer, comma-separated, default
127.0.0.1:1980. A PREDICT request names its model in its key, model.NAME,
and a multiplexer routes it only to a worker whose filter for PREDICT
takes the key: one that holds the model, or one started with --any-model,
which has no filter for PREDICT and so takes every model, loading each on
first use. A MODEL_CONTROL request names a worker in its key,
worker.NAME, and asks that worker to load or unload a model. The worker
answers once every multiplexer has its new filters and every request
routed under the old ones has been served, and only then frees a model
it unloaded: no request for it is on its way any more."""

import argparse
import re
import socket

from multiplexer.endpoints import parse_endpoint
from multiplexer.Multiplexer_pb2 import MultiplexerMessage
from multiplexer.servers import BaseMultiplexerServer

from multiplexer_constants import peers, types

NAME = re.compile(r"[a-z0-9][a-z0-9.-]*")  # what a model's or a worker's name may be


def model_key(model: str) -> str:
    """The key of a request for `model`."""
    return "model." + model


def worker_key(name: str) -> str:
    """The key of a request to the worker named `name`."""
    return "worker." + name


def only(key: str) -> str:
    """The pattern that takes `key` and no other: a multiplexer matches a
    pattern against the whole key, and re.escape() quotes the dots and
    dashes that RE2, as Python, would read as syntax."""
    return re.escape(key)


class Model:
    """A stand-in for a model. What the example is about is where models
    are held, so loading one takes nothing and its prediction is its input,
    marked with its name. A real one loads its weights here, which is what
    makes a worker hold only some, and runs them in predict()."""

    def __init__(self, name: str):
        self.name = name

    def predict(self, text: str) -> str:
        """The answer to `text`."""
        return f"{self.name}({text})"


class Worker(BaseMultiplexerServer):
    """Answers PREDICT for the models it holds, and MODEL_CONTROL addressed
    to it by name. Its key filters say which, from before it connects and
    then as it loads and unloads; a model it unloads it goes on serving
    until the change is acknowledged."""

    multiplexer_client_type = peers.MODEL_WORKER

    def __init__(self, addresses: list[tuple[str, int]], name: str, models: list[str] | None):
        """`models` are held from the start; None makes a worker that takes
        every model, loading each on first use."""
        super().__init__(addresses)
        self.name = name
        self.any_model = models is None
        self.models = {model: Model(model) for model in models or ()}
        # Unloaded and out of the filters, and still served: requests
        # routed under the old filters may be on their way until the
        # change is acknowledged.
        self.unloading: dict[str, Model] = {}
        self.served = 0
        self.set_key_filters(self.key_filters())  # before connecting, so that the welcome carries them

    def key_filters(self) -> dict[int, list[str]]:
        """The filters for what the worker holds now: its name for
        MODEL_CONTROL, and for PREDICT the models it holds, an empty list
        when it holds none, which takes no key. A worker that takes every
        model has no filter for PREDICT."""
        filters = {types.MODEL_CONTROL: [only(worker_key(self.name))]}
        if not self.any_model:
            filters[types.PREDICT] = [only(model_key(model)) for model in sorted(self.models)]
        return filters

    def handle_message(self, mxmsg: MultiplexerMessage) -> None:
        """A prediction, or a change of what the worker holds."""
        if mxmsg.type == types.PREDICT:
            self.predict(mxmsg)
        elif mxmsg.type == types.MODEL_CONTROL:
            self.control(mxmsg)
        else:
            self.no_response()

    def predict(self, mxmsg: MultiplexerMessage) -> None:
        """Answer with the model the key names: one held, one being
        unloaded, or, for a worker that takes every model, one loaded now."""
        name = mxmsg.key.removeprefix("model.")
        model = self.models.get(name) or self.unloading.get(name)
        if model is None and self.any_model and mxmsg.key == model_key(name) and NAME.fullmatch(name):
            model = self.models[name] = Model(name)
        if model is None:  # no key, or a request sent here with `to`
            self.report_error(f"{self.name} holds no model for the key {mxmsg.key!r}")
            return
        self.served += 1
        answer = model.predict(mxmsg.message.decode())
        self.send_message(message=f"{answer} by {self.name}", type=types.PREDICTION)

    def control(self, mxmsg: MultiplexerMessage) -> None:
        """Load or unload a model and change the filters to match; the
        answer waits for the change's acknowledgement (acknowledged())."""
        action, _, name = mxmsg.message.decode().partition(" ")
        if action not in ("load", "unload") or not NAME.fullmatch(name):
            self.report_error(f"{self.name} knows load MODEL and unload MODEL, not {mxmsg.message.decode()!r}")
            return
        unloaded = None
        if action == "load" and name not in self.models:
            self.models[name] = self.unloading.pop(name, None) or Model(name)
        elif action == "unload" and name in self.models:
            unloaded = self.unloading[name] = self.models.pop(name)
        self.set_key_filters(self.key_filters(), on_acknowledged=lambda: self.acknowledged(mxmsg, unloaded))
        self.no_response()

    def acknowledged(self, request: MultiplexerMessage, unloaded: Model | None) -> None:
        """The change `request` asked for is acknowledged: every
        multiplexer has the filters, and every request routed under the
        old ones has been served. A model it unloaded is freed, unless a
        load since took it back, since none of its requests can come any
        more; and the change is answered, in effect everywhere now. On the
        loop's thread, between two messages."""
        if unloaded is not None and self.unloading.get(unloaded.name) is unloaded:
            del self.unloading[unloaded.name]
        self.send_message(
            message=self.holding(),
            type=types.MODEL_STATUS,
            to=request.sender,
            references=request.id,
            workflow=request.workflow,
        )

    def holding(self) -> str:
        """What the worker holds, as MODEL_STATUS says it."""
        held = ", ".join(sorted(self.models)) or "no model"
        if self.any_model:
            return f"{self.name} takes every model, holds {held}"
        return f"{self.name} holds {held}"


def main() -> None:
    """Serve until killed."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument(
        "addresses", nargs="?", default="127.0.0.1:1980", help="host:port of every multiplexer, comma-separated"
    )
    parser.add_argument("--name", default=socket.gethostname().lower(), help="the worker's name; default the host's")
    held = parser.add_mutually_exclusive_group()
    held.add_argument("--models", default="", help="the models held from the start, comma-separated")
    held.add_argument(
        "--any-model", action="store_true", help="no filter for PREDICT: every model, loaded on first use"
    )
    args = parser.parse_args()
    models = None if args.any_model else [model for model in args.models.split(",") if model]
    for name in [args.name] + (models or []):
        if not NAME.fullmatch(name):
            parser.error(f"{name!r}: a name is lower-case letters, digits, dots and dashes")
    worker = Worker([parse_endpoint(text) for text in args.addresses.split(",")], args.name, models)
    worker.connect()  # so that the line below means reachable: whoever runs the steps, and the test, wait for it
    print(f"ready: {worker.holding()}", flush=True)
    worker.serve_forever()


if __name__ == "__main__":
    main()
