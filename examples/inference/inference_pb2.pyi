from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class InferRequest(_message.Message):
    __slots__ = ["image"]
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    image: bytes
    def __init__(self, image: _Optional[bytes] = ...) -> None: ...

class InferResponse(_message.Message):
    __slots__ = ["label", "probabilities", "seconds", "worker"]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    PROBABILITIES_FIELD_NUMBER: _ClassVar[int]
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    label: int
    probabilities: _containers.RepeatedScalarFieldContainer[float]
    seconds: float
    worker: str
    def __init__(
        self,
        label: _Optional[int] = ...,
        probabilities: _Optional[_Iterable[float]] = ...,
        worker: _Optional[str] = ...,
        seconds: _Optional[float] = ...,
    ) -> None: ...
