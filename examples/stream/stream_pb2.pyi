from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Cancel(_message.Message):
    __slots__ = ["stream_id"]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    stream_id: int
    def __init__(self, stream_id: _Optional[int] = ...) -> None: ...

class Generated(_message.Message):
    __slots__ = ["cancelled", "resent", "seconds", "stream_id", "tokens", "worker"]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    RESENT_FIELD_NUMBER: _ClassVar[int]
    SECONDS_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    cancelled: bool
    resent: int
    seconds: float
    stream_id: int
    tokens: int
    worker: str
    def __init__(self, stream_id: _Optional[int] = ..., tokens: _Optional[int] = ..., seconds: _Optional[float] = ..., worker: _Optional[str] = ..., cancelled: bool = ..., resent: _Optional[int] = ...) -> None: ...

class Prompt(_message.Message):
    __slots__ = ["from_seq", "prompt", "stream_id", "tokens"]
    FROM_SEQ_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    from_seq: int
    prompt: str
    stream_id: int
    tokens: int
    def __init__(self, stream_id: _Optional[int] = ..., prompt: _Optional[str] = ..., tokens: _Optional[int] = ..., from_seq: _Optional[int] = ...) -> None: ...

class Resume(_message.Message):
    __slots__ = ["from_seq", "stream_id", "until_seq"]
    FROM_SEQ_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    UNTIL_SEQ_FIELD_NUMBER: _ClassVar[int]
    from_seq: int
    stream_id: int
    until_seq: int
    def __init__(self, stream_id: _Optional[int] = ..., from_seq: _Optional[int] = ..., until_seq: _Optional[int] = ...) -> None: ...

class Resumed(_message.Message):
    __slots__ = ["done", "resent", "stream_id"]
    DONE_FIELD_NUMBER: _ClassVar[int]
    RESENT_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    done: bool
    resent: int
    stream_id: int
    def __init__(self, stream_id: _Optional[int] = ..., resent: _Optional[int] = ..., done: bool = ...) -> None: ...

class Token(_message.Message):
    __slots__ = ["last", "seq", "stream_id", "text"]
    LAST_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STREAM_ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    last: bool
    seq: int
    stream_id: int
    text: str
    def __init__(self, stream_id: _Optional[int] = ..., seq: _Optional[int] = ..., text: _Optional[str] = ..., last: bool = ...) -> None: ...
