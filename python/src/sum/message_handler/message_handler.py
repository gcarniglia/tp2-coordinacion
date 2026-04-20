import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        pass
    
    def serialize_data_message(self, client : str, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_AGG_DATA, client, [fruit, amount])

    def serialize_eof_message(self, client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_AGG_EOF, client, None)
    
    def deserialize_gateway_message(self, message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
