import hashlib
import os
import logging
import signal
import threading
from time import sleep

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
MAX_RETRY_EOF_CONSENSUS = 3

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

        self._sigterm_received = False
        self._runtime_error = False

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
        self._stop_lock = threading.Lock()
        self._stopping = False

        self.total_lines_counter_by_client = {}
        self.lines_processed_in_this_sum_from_gateway_by_client = {}
        self.lines_processed_from_other_sums_by_client = {}
        self.tries_processed_from_other_sums_by_client = {}

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()

    def _handle_runtime_failure(self, error, context):
        logging.error(f"{context}: {error}")
        self._runtime_error = True
        self.stop()

    def _run_gateway_consumer(self):
        try:
            self.gat_sum_queue.start_consuming(self.process_gateway_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Gateway consumer crashed")

    def _run_control_consumer(self):
        try:
            self.sum_control_consumer_exchange.start_consuming(self.process_sum_control_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")

    def _mark_client_finalized(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return False
            self._finalized_clients.add(client_id)
            return True

    def _finalize_client(self, client_id):
        logging.info(f"Finalizing client {client_id}")
        if not self._mark_client_finalized(client_id):
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
            self.lines_processed_in_this_sum_from_gateway_by_client[client_id] = self.lines_processed_in_this_sum_from_gateway_by_client.get(client_id, 0) + 1


    def _process_gateway_eof(self, client_id, total_lines):
        logging.info(f"Received GAT_SUM_EOF for client {client_id}")

        if SUM_AMOUNT > 1 and self.sum_control_producer_exchange is not None:
            self.total_lines_counter_by_client[client_id] = total_lines
            data = {
                "source_index": ID,
                "total_lines": total_lines,
                "lines_processed": self.lines_processed_in_this_sum_from_gateway_by_client.get(client_id, 0)
            }
            self.sum_control_producer_exchange.send(message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_EOF_REQ, client_id, data))
            logging.info(f"Sent SUM_EOF_REQ for client {client_id} to other sum filters")
        else:
            self._finalize_client(client_id)

    def _process_eof_request(self, client_id, source_index, total_lines, lines_processed):
        self.total_lines_counter_by_client[client_id] = total_lines
        self.lines_processed_from_other_sums_by_client.setdefault(client_id, {})
        self.lines_processed_from_other_sums_by_client[client_id][str(source_index)] = lines_processed
        with self._amount_by_fruit_lock:
            lines_processed_in_this_sum = self.lines_processed_in_this_sum_from_gateway_by_client.get(client_id, 0)
        self.sum_control_producer_exchange.send(message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_EOF_REP, client_id, {"source_index": ID, "lines_processed": lines_processed_in_this_sum}))

    def _process_eof_response(self, client_id, source_index, lines_processed):

        if client_id in self._finalized_clients:
            return

        self.lines_processed_from_other_sums_by_client.setdefault(client_id, {})
        self.lines_processed_from_other_sums_by_client[client_id][str(source_index)] = lines_processed

        if len(self.lines_processed_from_other_sums_by_client[client_id]) + 1 != SUM_AMOUNT:  # +1 por sum de este worker
            return

        if self._eof_consensus_achieved(client_id):
            logging.info(f"EOF consensus achieved for client {client_id}")
            logging.info(f"Total lines for client {client_id}: {self.total_lines_counter_by_client[client_id]}. Lines processed in this sum: {self.lines_processed_in_this_sum_from_gateway_by_client.get(client_id, 0)}. Lines processed from other sums: {self.lines_processed_from_other_sums_by_client.get(client_id, {})}")
            self._finalize_client(client_id)
        else:
            logging.info(f"EOF consensus not yet achieved for client {client_id}. Waiting for more responses.")
            logging.info(f"Total lines for client {client_id}: {self.total_lines_counter_by_client[client_id]}. Lines processed in this sum: {self.lines_processed_in_this_sum_from_gateway_by_client.get(client_id, 0)}. Lines processed from other sums: {self.lines_processed_from_other_sums_by_client.get(client_id, {})}")
            self.tries_processed_from_other_sums_by_client[client_id] = self.tries_processed_from_other_sums_by_client.get(client_id, 0) + 1
            self._retry_eof_consensus(client_id)

    def _retry_eof_consensus(self, client_id):
        tries = self.tries_processed_from_other_sums_by_client[client_id]
        sleep((tries+1) * 0.4)  # Exponential backoff
        if tries > MAX_RETRY_EOF_CONSENSUS:
            logging.warning(f"Max retries for EOF consensus exceeded for client {client_id}. Forcing finalization. This system is not failure-tolerant")
            self._finalize_client(client_id)
        else:
            logging.info(f"Retrying EOF consensus for client {client_id}. Attempt {tries+1}")
            with self._amount_by_fruit_lock:
                lines_processed_in_this_sum = self.lines_processed_in_this_sum_from_gateway_by_client.get(client_id, 0)
            self.sum_control_producer_exchange.send(message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SUM_EOF_REQ, client_id, {"source_index": ID, "total_lines": self.total_lines_counter_by_client[client_id], "lines_processed": lines_processed_in_this_sum}))

    def _eof_consensus_achieved(self, client_id):
        total_lines = self.total_lines_counter_by_client.get(client_id)
        if total_lines is None:
            return False
        with self._amount_by_fruit_lock:
            lines_processed_in_this_sum = self.lines_processed_in_this_sum_from_gateway_by_client.get(client_id, 0)
        lines_processed_from_other_sums = self.lines_processed_from_other_sums_by_client.get(client_id, {})
        total_reported = lines_processed_in_this_sum + sum(lines_processed_from_other_sums.values())
        return total_reported == total_lines

    def _flush_client_data(self, client_id):
        with self._amount_by_fruit_lock:
            if client_id in self.client_amounts_by_fruit:
                del self.client_amounts_by_fruit[client_id]
            if client_id in self.total_lines_counter_by_client:
                del self.total_lines_counter_by_client[client_id]
            if client_id in self.lines_processed_in_this_sum_from_gateway_by_client:
                del self.lines_processed_in_this_sum_from_gateway_by_client[client_id]
            if client_id in self.lines_processed_from_other_sums_by_client:
                del self.lines_processed_from_other_sums_by_client[client_id]
            if client_id in self.tries_processed_from_other_sums_by_client:
                del self.tries_processed_from_other_sums_by_client[client_id]

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

    def _try_finalize_client(self, client_id):
        with self._client_eof_state_lock:
            eof_pending = self._eof_pending_by_client.get(client_id, False)

        if eof_pending:
            self._finalize_client(client_id)

    def process_gateway_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.GAT_SUM_DATA:
                [fruit, amount] = message.data
                client_id = message.source_client_uuid
                self._process_data(fruit, amount, client_id)
                self._try_finalize_client(client_id)
            case message_protocol.internal.InternalMessageType.GAT_SUM_EOF:
                client_id = message.source_client_uuid
                self._process_gateway_eof(client_id,message.data)
        ack()

    def process_sum_control_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SUM_EOF_REQ:
                logging.info(f"Received SUM_EOF_REQ for client {message.source_client_uuid} from sum filter index {message.data['source_index']}")
                self._process_eof_request(message.source_client_uuid, message.data["source_index"], message.data["total_lines"], message.data["lines_processed"])
            case message_protocol.internal.InternalMessageType.SUM_EOF_REP:
                logging.info(f"Received SUM_EOF_REP for client {message.source_client_uuid} from sum filter index {message.data['source_index']}")
                self._process_eof_response(message.source_client_uuid, message.data["source_index"], message.data["lines_processed"])
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
        target=self._run_gateway_consumer,
        name="gateway-consumer-thread",
        )
        control_thread = None
        if SUM_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
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

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if gateway_started:
            gateway_thread.join()
        if control_started:
            control_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in sum")
        sum_filter.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return sum_filter.start()


if __name__ == "__main__":
    main()
