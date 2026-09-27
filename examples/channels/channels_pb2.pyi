from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ChannelEnvelope(_message.Message):
    __slots__ = ["channel", "group", "payload", "seq", "stream"]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STREAM_FIELD_NUMBER: _ClassVar[int]
    channel: str
    group: str
    payload: bytes
    seq: int
    stream: int
    def __init__(self, channel: _Optional[str] = ..., group: _Optional[str] = ..., payload: _Optional[bytes] = ..., stream: _Optional[int] = ..., seq: _Optional[int] = ...) -> None: ...
