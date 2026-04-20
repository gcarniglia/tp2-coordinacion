import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_CONTROL_EXCHANGE = "AGGREGATION_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.data_agg_exchanges = []

        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_agg_exchanges.append(data_output_exchange)
        
        self.broadcast_agg_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_CONTROL_EXCHANGE, [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)]
            )

        self.sum_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, 
            SUM_CONTROL_EXCHANGE, 
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT) if i != ID]
        )

        self.amount_by_fruit = {}

    def _process_data(self, fruit, amount,client_id):

        self.amount_by_fruit[(client_id, fruit)] = self.amount_by_fruit.get(
            (client_id, fruit), fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))


    def _process_eof(self, client_id):
        self.sum_control_exchange.send(message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_SUM_EOF, client_id, None))
        self._send_to_aggregators(client_id)

    def _send_to_aggregators(self, client_id):
        logging.info(f"Broadcasting data messages")
        for (cid, _), final_fruit_item in self.amount_by_fruit.items():
            if cid != client_id:
                continue
            index = hash(client_id + final_fruit_item.fruit) % AGGREGATION_AMOUNT
            self.data_agg_exchanges[index].send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.SUM_AGG_DATA, 
                    client_id, 
                    [final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF message")
        self.broadcast_agg_exchange.send(message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_AGG_EOF, client_id, None))

    def _process_shutdown(self):
        #TODO: yo no recibo este mensaje. Lo que recibo es un cierre del 
        # canal de rabbitmq, que es lo que me indica que no van a venir mas mensajes. Entonces, en vez de procesar un mensaje de shutdown, lo que hago es cerrar el canal y salir del programa.
        return


    def process_gateway_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.GAT_SUM_DATA:
                [fruit, amount] = message.data
                client_id = message.source_client_uuid
                self._process_data(fruit, amount, client_id)
            case message_protocol.internal.InternalMessageType.GAT_SUM_EOF:
                client_id = message.source_client_uuid
                self._process_eof(client_id)
        ack()

    def process_sum_control_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SUM_SUM_EOF:
                self._send_to_aggregators(message.source_client_uuid)
        ack()


    def start(self):
        self.input_queue.start_consuming(self.process_gateway_messages)
        self.sum_control_exchange.start_consuming(self.process_sum_control_messages)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
