import os
import logging
import threading
import signal

from common import middleware, message_protocol, fruit_item

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(message)s',
            datefmt='%H:%M:%S'
        )

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self._sigterm_received = False
        self._runtime_error = False

        self.clients_top_3 = {}
        self.top_3_lock = threading.Lock()
        
        self._stop_lock = threading.Lock()
        self._stopping = False

        self._top_size = TOP_SIZE
        
        self._eof_count_lock = threading.Lock()
        self._eof_count_by_client = {}
    
    def _run_consumer(self):
        try:
            self.input_queue.start_consuming(self.process_message)
        except Exception as e:
            logging.error(f"Join consumer crashed: {e}")
            with self._stop_lock:
                if not self._stopping:
                    self._runtime_error = True
            self.stop()
    
    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()

    def _process_aggregation_data(self, client_id, data):
        [fruit, amount] = data
        new_item = fruit_item.FruitItem(fruit, int(amount))

        with self.top_3_lock:
            client_top_3 = self.clients_top_3.setdefault(client_id, [])
            if len(client_top_3) < self._top_size or new_item > client_top_3[-1]:
                client_top_3.append(new_item)
                client_top_3.sort(reverse=True)
                if len(client_top_3) > self._top_size:
                    client_top_3.pop()

    def _process_eof(self, client_id):
        
        with self._eof_count_lock:
            eof_count = self._eof_count_by_client.get(client_id, 0) + 1
            self._eof_count_by_client[client_id] = eof_count
            if eof_count != AGGREGATION_AMOUNT:
                return

        with self.top_3_lock:
            client_top_3 = list(self.clients_top_3.get(client_id, []))

        fruit_top = [[item.fruit, item.amount] for item in client_top_3]
        
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.JOIN_GAT_DATA,
                client_id,
                fruit_top,
            )
        )
        logging.info(
            "Sent JOIN_GAT_DATA for client %s: %s", client_id, fruit_top
        )

        with self.top_3_lock:
            self.clients_top_3.pop(client_id, None)
        with self._eof_count_lock:
            self._eof_count_by_client.pop(client_id, None)


    def process_message(self, message, ack, nack):
        
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.AGG_JOIN_DATA:
                client_id = message.source_client_uuid
                data = message.data
                logging.info(
                    "Received AGG_JOIN_DATA for client %s: %s", client_id, data
                )
                self._process_aggregation_data(client_id, data)
            case message_protocol.internal.InternalMessageType.AGG_JOIN_EOF:
                client_id = message.source_client_uuid
                logging.info("Received AGG_JOIN_EOF for client %s", client_id)
                self._process_eof(client_id)
        ack()

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.input_queue, self.output_queue]

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def start(self):
        consumer_thread = threading.Thread(
            target=self._run_consumer,
            name="join-consumer-thread",
        )

        consumer_started = False

        try:
            consumer_thread.start()
            consumer_started = True
        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if consumer_started:
            consumer_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in join")
        join_filter.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    return join_filter.start()


if __name__ == "__main__":
    main()
