from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Click(_message.Message):
    __slots__ = ["page", "time_ns", "user"]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    TIME_NS_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    page: str
    time_ns: int
    user: str
    def __init__(self, user: _Optional[str] = ..., page: _Optional[str] = ..., time_ns: _Optional[int] = ...) -> None: ...

class Counts(_message.Message):
    __slots__ = ["clicks", "pages"]
    class PagesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    CLICKS_FIELD_NUMBER: _ClassVar[int]
    PAGES_FIELD_NUMBER: _ClassVar[int]
    clicks: int
    pages: _containers.ScalarMap[str, int]
    def __init__(self, pages: _Optional[_Mapping[str, int]] = ..., clicks: _Optional[int] = ...) -> None: ...
