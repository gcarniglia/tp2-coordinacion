import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        pass
    
    def serialize_data_message(self, client, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.JOIN_GAT_DATA, client, [fruit, amount])

    def deserialize_aggregation_message(self, message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
