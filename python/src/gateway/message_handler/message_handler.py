import uuid
import logging

from common import message_protocol

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(message)s',
            datefmt='%H:%M:%S'
        )

class MessageHandler:

    def __init__(self):
        self.client_uuid = str(uuid.uuid4())
        self.line_counter_by_client = {}
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.line_counter_by_client[self.client_uuid] = self.line_counter_by_client.get(self.client_uuid, 0) + 1
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.GAT_SUM_DATA, self.client_uuid, [fruit, amount])

    def serialize_eof_message(self, message): #originalmente venía nada de aca
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.GAT_SUM_EOF, self.client_uuid, self.line_counter_by_client.get(self.client_uuid, 0))

    def deserialize_result_message(self, message):
        internal_message = message_protocol.internal.deserialize(message)
        if (
            internal_message.type
            != message_protocol.internal.InternalMessageType.JOIN_GAT_DATA
        ):
            return None

        # Only consume the result that belongs to this gateway-side client handler.
        if internal_message.source_client_uuid != self.client_uuid:
            return None

        logging.info(
            "Client %s received JOIN_GAT message with data %s",
            internal_message.source_client_uuid,
            internal_message.data,
        )
        return internal_message.data
