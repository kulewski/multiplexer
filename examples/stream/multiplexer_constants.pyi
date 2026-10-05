#
# this file is generated from stream.rules (mxcontrol generate_constants); do not edit
#

RULES_FINGERPRINT: str

class _constants_base:
    idtoname: dict[int, str]
    @classmethod
    def get_name(cls, type: int, default: str = ...) -> str: ...

class types(_constants_base):
    PING: int
    CONNECTION_WELCOME: int
    BACKEND_FOR_PACKET_SEARCH: int
    HEARTBIT: int
    DELIVERY_ERROR: int
    RECORDING_CONTROL: int
    RECORDING_STATUS: int
    RECORDING_RECORD: int
    RULES_CONTROL: int
    RULES_STATUS: int
    PEER_CONTROL: int
    PEER_STATUS: int
    MAX_MULTIPLEXER_META_PACKET: int
    PICKLE_RESPONSE: int
    REQUEST_RECEIVED: int
    BACKEND_ERROR: int
    LOGS_STREAM: int
    GENERATE: int
    TOKEN: int
    GENERATED: int
    RESUME: int
    RESUMED: int
    CANCEL: int

class peers(_constants_base):
    MULTIPLEXER: int
    ALL_TYPES: int
    MAX_MULTIPLEXER_SPECIAL_PEER_TYPE: int
    LOG_STREAMER: int
    LOG_RECEIVER_EXAMPLE: int
    STREAM_CLIENT: int
    GENERATOR: int

