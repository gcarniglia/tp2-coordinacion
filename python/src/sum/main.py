import hashlib
import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(message)s',
            datefmt='%H:%M:%S'
        )

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
        self.gat_sum_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.data_sum_agg_exchanges = []

        for i in range(AGGREGATION_AMOUNT):
            data_sum_agg_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_sum_agg_exchanges.append(data_sum_agg_exchange)
        


        self.sum_control_consumer_exchange = None
        self.sum_control_producer_exchange = None
        if SUM_AMOUNT > 1:
            # Consume only messages addressed to this sum instance.
            self.sum_control_consumer_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [f"{SUM_PREFIX}_{ID}"],
            )
            # Publish control EOF notifications to all other sum instances.
            self.sum_control_producer_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT) if i != ID],
            )

        # amount_by_fruit[client_id][fruit] = FruitItem
        self.client_amounts_by_fruit = {}
        self._amount_by_fruit_lock = threading.Lock()
        self._aggregator_publish_lock = threading.Lock()
        self._finalized_clients = set()
        self._finalized_clients_lock = threading.Lock()
        self._client_eof_state_lock = threading.Lock()
        self._eof_pending_by_client = {}
        self._inflight_by_client = {}
        self._stop_lock = threading.Lock()
        self._stopping = False

    def _mark_client_finalized(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return False
            self._finalized_clients.add(client_id)
            return True

    def _mark_eof_pending(self, client_id):
        with self._client_eof_state_lock:
            self._eof_pending_by_client[client_id] = True

    def _increment_client_inflight(self, client_id):
        with self._client_eof_state_lock:
            current = self._inflight_by_client.get(client_id, 0)
            self._inflight_by_client[client_id] = current + 1

    def _decrement_client_inflight(self, client_id):
        with self._client_eof_state_lock:
            current = self._inflight_by_client.get(client_id, 0)
            if current <= 1:
                self._inflight_by_client.pop(client_id, None)
                return
            self._inflight_by_client[client_id] = current - 1

    def _try_finalize_client(self, client_id):
        with self._client_eof_state_lock:
            if not self._eof_pending_by_client.get(client_id, False):
                return
            if self._inflight_by_client.get(client_id, 0) != 0:
                return
            self._eof_pending_by_client.pop(client_id, None)
            self._inflight_by_client.pop(client_id, None)

        self._finalize_client(client_id)

    def _finalize_client(self, client_id):
        if not self._mark_client_finalized(client_id):
            logging.info(f"Client {client_id} was already finalized")
            return
        self._send_to_aggregators(client_id)
        self._flush_client_data(client_id)

    def _process_data(self, fruit, amount, client_id):
        logging.info(f"Received GAT_SUM_DATA for client {client_id} and fruit {fruit} with amount {amount}")
        with self._amount_by_fruit_lock:
            client_amounts = self.client_amounts_by_fruit.setdefault(client_id, {})
            client_amounts[fruit] = client_amounts.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))


    def _process_eof(self, client_id):
        logging.info(f"Received GAT_SUM_EOF for client {client_id}")
        
        if SUM_AMOUNT > 1 and self.sum_control_producer_exchange is not None:
            self.sum_control_producer_exchange.send(message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_SUM_EOF, client_id, None))
            logging.info(f"Sent SUM_SUM_EOF for client {client_id} to other sum filters")

        self._mark_eof_pending(client_id)
        self._try_finalize_client(client_id)
    
    def _flush_client_data(self, client_id):
        with self._amount_by_fruit_lock:
            if client_id in self.client_amounts_by_fruit:
                del self.client_amounts_by_fruit[client_id]

    def _send_to_aggregators(self, client_id):
        with self._amount_by_fruit_lock:
            client_amounts = self.client_amounts_by_fruit.get(client_id, {})

        logging.info(f"Starting to send SUM_AGG_DATA messages for client {client_id} to aggregators")
        with self._aggregator_publish_lock:
            for final_fruit_item in client_amounts.values():
                designated_agg = self.worker_for(client_id, final_fruit_item.fruit)
                self.data_sum_agg_exchanges[designated_agg].send(
                    message_protocol.internal.serialize(
                        message_protocol.internal.InternalMessageType.SUM_AGG_DATA,
                        client_id,
                        [final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )
                logging.info(f"Sent SUM_AGG_DATA for client {client_id} and fruit {final_fruit_item.fruit} to aggregator {designated_agg}")

            self._broadcast_sum_agg_exchange(client_id)

    
    def worker_for(self, client_id, fruit) -> int:

        # representación canónica y estable
        key = f"{client_id.lower()}|{fruit.lower()}".encode("utf-8")

        # hash estable
        digest = hashlib.sha256(key).digest()

        # convertir a entero
        value = int.from_bytes(digest, byteorder="big")

        # asignación al worker
        return value % AGGREGATION_AMOUNT

    def _broadcast_sum_agg_exchange(self, client_id):
        logging.info(f"Broadcasting SUM_AGG_EOF for client {client_id} to all aggregators")
        for exchange in self.data_sum_agg_exchanges:
            exchange.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.SUM_AGG_EOF, 
                    client_id, 
                    None
                )
            )


    def process_gateway_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.GAT_SUM_DATA:
                [fruit, amount] = message.data
                client_id = message.source_client_uuid
                self._increment_client_inflight(client_id)
                try:
                    self._process_data(fruit, amount, client_id)
                finally:
                    self._decrement_client_inflight(client_id)
                self._try_finalize_client(client_id)
            case message_protocol.internal.InternalMessageType.GAT_SUM_EOF:
                client_id = message.source_client_uuid
                self._process_eof(client_id)
        ack()

    def process_sum_control_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SUM_SUM_EOF:
                logging.info(f"Received SUM_SUM_EOF for client {message.source_client_uuid} from another sum filter")
                self._mark_eof_pending(message.source_client_uuid)
                self._try_finalize_client(message.source_client_uuid)
        ack()

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.gat_sum_queue]
        if self.sum_control_consumer_exchange is not None:
            consumers.append(self.sum_control_consumer_exchange)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.gat_sum_queue, *self.data_sum_agg_exchanges]
        if self.sum_control_consumer_exchange is not None:
            resources.append(self.sum_control_consumer_exchange)
        if self.sum_control_producer_exchange is not None:
            resources.append(self.sum_control_producer_exchange)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def start(self):
        gateway_thread = threading.Thread(
            target=self.gat_sum_queue.start_consuming,
            args=(self.process_gateway_messages,),
            name="gateway-consumer-thread",
        )
        control_thread = None
        if SUM_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self.sum_control_consumer_exchange.start_consuming,
                args=(self.process_sum_control_messages,),
                name="sum-control-consumer-thread",
            )

        gateway_started = False
        control_started = False

        try:
            gateway_thread.start()
            gateway_started = True
            if SUM_AMOUNT > 1:
                control_thread.start()
                control_started = True

        except Exception:
            self.stop()
            raise

        finally:
            if gateway_started:
                gateway_thread.join()
            if control_started:
                control_thread.join()
            self._close_resources()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in sum")
        sum_filter.stop()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
