#
# this file is generated from cache.rules (mxcontrol generate_constants); do not edit
#

# CRC-32 of the rules file these constants were generated from; a recording's header carries the same.
RULES_FINGERPRINT = "b904350a"


class _constants_base:
    idtoname = None  # dict defined by a subclass

    @classmethod
    def get_name(cls, type, default="UNKNOWN"):
        return cls.idtoname.get(type, default)


class types(_constants_base):

    PING = 1
    CONNECTION_WELCOME = 2
    BACKEND_FOR_PACKET_SEARCH = 3
    HEARTBIT = 4
    DELIVERY_ERROR = 5
    RECORDING_CONTROL = 6
    RECORDING_STATUS = 7
    RECORDING_RECORD = 8
    RULES_CONTROL = 9
    RULES_STATUS = 10
    PEER_CONTROL = 11
    PEER_STATUS = 12
    MAX_MULTIPLEXER_META_PACKET = 99
    PICKLE_RESPONSE = 112
    REQUEST_RECEIVED = 113
    BACKEND_ERROR = 114
    LOGS_STREAM = 115
    CACHE_GET = 301
    CACHE_VALUE = 302
    CACHE_SET = 303
    CACHE_DELETE = 304
    CACHE_CLEAR = 305
    CACHE_ADD = 306
    CACHE_RESULT = 307
    CACHE_STATS_REQUEST = 308
    CACHE_STATS = 309

    idtoname = {}
    idtoname[PING] = "PING"
    idtoname[CONNECTION_WELCOME] = "CONNECTION_WELCOME"
    idtoname[BACKEND_FOR_PACKET_SEARCH] = "BACKEND_FOR_PACKET_SEARCH"
    idtoname[HEARTBIT] = "HEARTBIT"
    idtoname[DELIVERY_ERROR] = "DELIVERY_ERROR"
    idtoname[RECORDING_CONTROL] = "RECORDING_CONTROL"
    idtoname[RECORDING_STATUS] = "RECORDING_STATUS"
    idtoname[RECORDING_RECORD] = "RECORDING_RECORD"
    idtoname[RULES_CONTROL] = "RULES_CONTROL"
    idtoname[RULES_STATUS] = "RULES_STATUS"
    idtoname[PEER_CONTROL] = "PEER_CONTROL"
    idtoname[PEER_STATUS] = "PEER_STATUS"
    idtoname[MAX_MULTIPLEXER_META_PACKET] = "MAX_MULTIPLEXER_META_PACKET"
    idtoname[PICKLE_RESPONSE] = "PICKLE_RESPONSE"
    idtoname[REQUEST_RECEIVED] = "REQUEST_RECEIVED"
    idtoname[BACKEND_ERROR] = "BACKEND_ERROR"
    idtoname[LOGS_STREAM] = "LOGS_STREAM"
    idtoname[CACHE_GET] = "CACHE_GET"
    idtoname[CACHE_VALUE] = "CACHE_VALUE"
    idtoname[CACHE_SET] = "CACHE_SET"
    idtoname[CACHE_DELETE] = "CACHE_DELETE"
    idtoname[CACHE_CLEAR] = "CACHE_CLEAR"
    idtoname[CACHE_ADD] = "CACHE_ADD"
    idtoname[CACHE_RESULT] = "CACHE_RESULT"
    idtoname[CACHE_STATS_REQUEST] = "CACHE_STATS_REQUEST"
    idtoname[CACHE_STATS] = "CACHE_STATS"


class peers(_constants_base):

    MULTIPLEXER = 1
    ALL_TYPES = 2
    MAX_MULTIPLEXER_SPECIAL_PEER_TYPE = 99
    LOG_STREAMER = 108
    LOG_RECEIVER_EXAMPLE = 111
    CACHE_CLIENT = 201
    CACHE = 202
    CACHE_JOURNAL = 203

    idtoname = {}
    idtoname[MULTIPLEXER] = "MULTIPLEXER"
    idtoname[ALL_TYPES] = "ALL_TYPES"
    idtoname[MAX_MULTIPLEXER_SPECIAL_PEER_TYPE] = "MAX_MULTIPLEXER_SPECIAL_PEER_TYPE"
    idtoname[LOG_STREAMER] = "LOG_STREAMER"
    idtoname[LOG_RECEIVER_EXAMPLE] = "LOG_RECEIVER_EXAMPLE"
    idtoname[CACHE_CLIENT] = "CACHE_CLIENT"
    idtoname[CACHE] = "CACHE"
    idtoname[CACHE_JOURNAL] = "CACHE_JOURNAL"
