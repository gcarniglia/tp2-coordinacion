import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_uuid = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.GAT_SUM_DATA, self.client_uuid, [fruit, amount])

    def serialize_eof_message(self, message): #originalmente venía nada de aca
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.GAT_SUM_EOF, self.client_uuid, None)

    def deserialize_result_message(self, message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message.data
