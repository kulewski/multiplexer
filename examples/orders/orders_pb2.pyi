from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Order(_message.Message):
    __slots__ = ["customer", "id", "item", "quantity"]
    CUSTOMER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    customer: str
    id: str
    item: str
    quantity: int
    def __init__(self, id: _Optional[str] = ..., customer: _Optional[str] = ..., item: _Optional[str] = ..., quantity: _Optional[int] = ...) -> None: ...
