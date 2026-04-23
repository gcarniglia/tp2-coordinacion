import os
import logging
import bisect
import threading
import signal

from common import middleware, message_protocol, fruit_item

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(message)s',
            datefmt='%H:%M:%S'
        )

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.data_sum_agg_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )

        self.agg_join_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self._sigterm_received = False
        self._runtime_error = False

        self.client_amounts_by_fruit = {}
        self._amount_by_fruit_lock = threading.Lock()
        
        self._stop_lock = threading.Lock()
        self._stopping = False

        self._top_k_size = TOP_SIZE

        self._eof_count_lock = threading.Lock()
        self._eof_count_by_client = {}

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()

    def _run_sum_consumer(self):
        try:
            self.data_sum_agg_exchange.start_consuming(self.process_sum_messages)
        except Exception as e:
            logging.error(f"SUM->AGG consumer crashed: {e}")
            with self._stop_lock:
                if not self._stopping:
                    self._runtime_error = True
            self.stop()

    def _process_data(self, fruit_total, amount, client_id):
        logging.info("Received SUM_AGG_DATA for client %s and fruit %s with amount %d", client_id, fruit_total, amount)

        with self._amount_by_fruit_lock:
            client_top_fruits = self.client_amounts_by_fruit.setdefault(client_id, [])

            # Update existing fruit amount, otherwise keep list ordered with bisect.
            for i in range(len(client_top_fruits)):
                if client_top_fruits[i].fruit == fruit_total:
                    updated_item = client_top_fruits[i] + fruit_item.FruitItem(
                        fruit_total, amount
                    )
                    del client_top_fruits[i]
                    bisect.insort(client_top_fruits, updated_item)
                    return
            bisect.insort(client_top_fruits, fruit_item.FruitItem(fruit_total, amount))

    def _process_eof(self, client_id):
        logging.info("Received SUM_AGG_EOF for client %s", client_id)

        with self._eof_count_lock:
            eof_count = self._eof_count_by_client.get(client_id, 0) + 1
            self._eof_count_by_client[client_id] = eof_count
            if eof_count != SUM_AMOUNT:
                return

        with self._amount_by_fruit_lock:
            client_top_fruits = list(self.client_amounts_by_fruit.get(client_id, []))

        fruit_chunk = list(client_top_fruits[-self._top_k_size:])
        fruit_chunk.reverse()
        fruit_top = [(item.fruit, item.amount) for item in fruit_chunk]

        for (fruit,amount) in fruit_top:
            self.agg_join_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.AGG_JOIN_DATA,
                    client_id,
                    [fruit,amount],
                )
            )
            logging.info(f"Sent AGG_JOIN_DATA message for client {client_id} and fruit {fruit} to join filter")

        self.agg_join_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.AGG_JOIN_EOF,
                client_id,
                None,
            )
        )
        logging.info(f"Sent AGG_JOIN_EOF message for client {client_id} to join filter")

        with self._amount_by_fruit_lock:
            self.client_amounts_by_fruit.pop(client_id, None)
        del self._eof_count_by_client[client_id]

    def process_sum_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SUM_AGG_DATA:
                [fruit_total, amount] = message.data
                client_id = message.source_client_uuid
                self._process_data(fruit_total, amount, client_id)
            case message_protocol.internal.InternalMessageType.SUM_AGG_EOF:
                client_id = message.source_client_uuid
                self._process_eof(client_id)
        ack()

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        for consumer in [self.data_sum_agg_exchange]:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [
            self.data_sum_agg_exchange,
            self.agg_join_queue,
        ]

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def start(self):
        data_thread = threading.Thread(
            target=self._run_sum_consumer,
            name="sum-agg-data-consumer-thread",
        )

        data_started = False

        try:
            data_thread.start()
            data_started = True
        except Exception as e:
            self.stop()
            logging.error(e)
            self._close_resources()
            return 2

        if data_started:
            data_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in aggregation")
        aggregation_filter.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return aggregation_filter.start()


if __name__ == "__main__":
    main()
