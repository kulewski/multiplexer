from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class AudioFrame(_message.Message):
    __slots__ = ["captured_at", "effect", "participant", "pcm", "seq"]
    CAPTURED_AT_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    PARTICIPANT_FIELD_NUMBER: _ClassVar[int]
    PCM_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    captured_at: float
    effect: str
    participant: int
    pcm: bytes
    seq: int
    def __init__(self, participant: _Optional[int] = ..., seq: _Optional[int] = ..., effect: _Optional[str] = ..., pcm: _Optional[bytes] = ..., captured_at: _Optional[float] = ...) -> None: ...

class AudioProcessed(_message.Message):
    __slots__ = ["captured_at", "micros", "participant", "pcm", "seq", "spectrum", "worker"]
    CAPTURED_AT_FIELD_NUMBER: _ClassVar[int]
    MICROS_FIELD_NUMBER: _ClassVar[int]
    PARTICIPANT_FIELD_NUMBER: _ClassVar[int]
    PCM_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    SPECTRUM_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    captured_at: float
    micros: int
    participant: int
    pcm: bytes
    seq: int
    spectrum: bytes
    worker: str
    def __init__(self, participant: _Optional[int] = ..., seq: _Optional[int] = ..., pcm: _Optional[bytes] = ..., spectrum: _Optional[bytes] = ..., worker: _Optional[str] = ..., micros: _Optional[int] = ..., captured_at: _Optional[float] = ...) -> None: ...
