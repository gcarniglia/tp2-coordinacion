import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        pass
    
    def serialize_data_message(self, client, list_of_subtops):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.AGG_JOIN_DATA, client, list_of_subtops)

    def serialize_eof_message(self, client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.AGG_JOIN_EOF, client, None)

    def serialize_shutdown_message(self):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.AGG_JOIN_SHUT, None, None)

    def deserialize_sum_message(self, message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
