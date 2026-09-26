from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class CacheClear(_message.Message):
    __slots__ = ["version", "writer"]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_FIELD_NUMBER: _ClassVar[int]
    version: int
    writer: int
    def __init__(self, version: _Optional[int] = ..., writer: _Optional[int] = ...) -> None: ...

class CacheEntry(_message.Message):
    __slots__ = ["expires", "key", "value", "version", "writer"]
    EXPIRES_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_FIELD_NUMBER: _ClassVar[int]
    expires: float
    key: str
    value: bytes
    version: int
    writer: int
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ..., expires: _Optional[float] = ..., version: _Optional[int] = ..., writer: _Optional[int] = ...) -> None: ...

class CacheKey(_message.Message):
    __slots__ = ["key", "version", "writer"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_FIELD_NUMBER: _ClassVar[int]
    key: str
    version: int
    writer: int
    def __init__(self, key: _Optional[str] = ..., version: _Optional[int] = ..., writer: _Optional[int] = ...) -> None: ...

class CacheResult(_message.Message):
    __slots__ = ["done", "replica"]
    DONE_FIELD_NUMBER: _ClassVar[int]
    REPLICA_FIELD_NUMBER: _ClassVar[int]
    done: bool
    replica: str
    def __init__(self, done: bool = ..., replica: _Optional[str] = ...) -> None: ...

class CacheStats(_message.Message):
    __slots__ = ["entries", "hits", "instance_id", "reads", "replica", "request_id", "writes"]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    HITS_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    READS_FIELD_NUMBER: _ClassVar[int]
    REPLICA_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    WRITES_FIELD_NUMBER: _ClassVar[int]
    entries: int
    hits: int
    instance_id: int
    reads: int
    replica: str
    request_id: int
    writes: int
    def __init__(self, request_id: _Optional[int] = ..., replica: _Optional[str] = ..., instance_id: _Optional[int] = ..., entries: _Optional[int] = ..., writes: _Optional[int] = ..., reads: _Optional[int] = ..., hits: _Optional[int] = ...) -> None: ...

class CacheStatsRequest(_message.Message):
    __slots__ = ["request_id"]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    def __init__(self, request_id: _Optional[int] = ...) -> None: ...

class CacheValue(_message.Message):
    __slots__ = ["found", "replica", "value"]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    REPLICA_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    found: bool
    replica: str
    value: bytes
    def __init__(self, found: bool = ..., value: _Optional[bytes] = ..., replica: _Optional[str] = ...) -> None: ...
