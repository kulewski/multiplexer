#
# this file is generated from channels.rules (mxcontrol generate_constants); do not edit
#

# CRC-32 of the rules file these constants were generated from; a recording's header carries the same.
RULES_FINGERPRINT = "2c1786c2"


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
    CHANNEL_GROUP_SEND = 301
    CHANNEL_SEND = 302

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
    idtoname[CHANNEL_GROUP_SEND] = "CHANNEL_GROUP_SEND"
    idtoname[CHANNEL_SEND] = "CHANNEL_SEND"


class peers(_constants_base):

    MULTIPLEXER = 1
    ALL_TYPES = 2
    MAX_MULTIPLEXER_SPECIAL_PEER_TYPE = 99
    LOG_STREAMER = 108
    LOG_RECEIVER_EXAMPLE = 111
    CHANNELS = 201

    idtoname = {}
    idtoname[MULTIPLEXER] = "MULTIPLEXER"
    idtoname[ALL_TYPES] = "ALL_TYPES"
    idtoname[MAX_MULTIPLEXER_SPECIAL_PEER_TYPE] = "MAX_MULTIPLEXER_SPECIAL_PEER_TYPE"
    idtoname[LOG_STREAMER] = "LOG_STREAMER"
    idtoname[LOG_RECEIVER_EXAMPLE] = "LOG_RECEIVER_EXAMPLE"
    idtoname[CHANNELS] = "CHANNELS"
