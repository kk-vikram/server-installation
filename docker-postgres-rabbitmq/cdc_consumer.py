import json
import pika
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import time
import os
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = int(os.getenv('PG_PORT', 5432))
PG_DB = os.getenv('PG_DB', 'inventory')
PG_USER = os.getenv('PG_USER', 'postgres')
PG_PASS = os.getenv('PG_PASS', 'postgres')
SLOT_NAME = 'inventory_slot'
RMQ_HOST = os.getenv('RMQ_HOST', 'localhost')
RMQ_PORT = int(os.getenv('RMQ_PORT', 5672))
RMQ_USER = os.getenv('RMQ_USER', 'guest')
RMQ_PASS = os.getenv('RMQ_PASS', 'guest')
QUEUE = os.getenv('QUEUE', 'dbserver1.public.customers')

def connect_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RMQ_HOST,
                    port=RMQ_PORT,
                    credentials=pika.PlainCredentials(RMQ_USER, RMQ_PASS),
                    heartbeat=600
                )
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            logger.debug(f"Connected to RabbitMQ at {RMQ_HOST}:{RMQ_PORT}, queue {QUEUE}")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"RabbitMQ connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

def publish_to_rmq(channel, message):
    try:
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logger.debug(f"Published to RabbitMQ: {message}")
    except pika.exceptions.AMQPError as e:
        logger.error(f"Failed to publish to RabbitMQ: {e}")
        raise

class CDCConsumer:
    def __init__(self):
        self.conn = None
        self.cur = None
        self.rmq_conn = None
        self.rmq_channel = None
        self.connect_postgres()
        self.connect_rabbitmq()

    def connect_postgres(self):
        while True:
            try:
                self.conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, database=PG_DB,
                    user=PG_USER, password=PG_PASS,
                    connection_factory=LogicalReplicationConnection
                )
                self.cur = self.conn.cursor()
                logger.debug(f"Connected to PostgreSQL at {PG_HOST}:{PG_PORT}, database {PG_DB}")
                return
            except psycopg2.OperationalError as e:
                logger.error(f"PostgreSQL connection failed: {e}. Retrying in 5s...")
                time.sleep(5)

    def connect_rabbitmq(self):
        self.rmq_conn, self.rmq_channel = connect_rabbitmq()
        logger.debug("RabbitMQ connection established")

    def handle_event(self, payload):
        try:
            data = json.loads(payload.decode('utf-8'))
            logger.debug(f"Received WAL payload: {data}")
            
            # Handle wal2json format with 'action' key
            if data.get('action') in ('I', 'U', 'D'):  # Insert, Update, Delete
                if data.get('schema') == 'public' and data.get('table') == 'customers':
                    event = {
                        'table': f"{data['schema']}.{data['table']}",
                        'operation': {'I': 'insert', 'U': 'update', 'D': 'delete'}.get(data['action']),
                        'data': {col['name']: col['value'] for col in data.get('columns', [])} if 'columns' in data else {},
                        'old': {col['name']: col['value'] for col in data.get('identity', [])} if data.get('action') in ('U', 'D') else None,
                        'timestamp': data.get('timestamp')
                    }
                    publish_to_rmq(self.rmq_channel, event)
                else:
                    logger.debug(f"Skipping change for table {data['schema']}.{data['table']}")
            else:
                logger.debug(f"Skipping non-data event: {data['action']}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse wal2json payload: {e}")
        except pika.exceptions.AMQPError:
            logger.warning("RabbitMQ connection lost. Reconnecting...")
            self.connect_rabbitmq()

    def start(self):
        try:
            # Create or reuse replication slot
            self.cur.execute("SELECT * FROM pg_create_logical_replication_slot(%s, %s);",
                            (SLOT_NAME, 'wal2json'))
            logger.debug(f"Created/using replication slot {SLOT_NAME}")
            self.cur.start_replication(
                slot_name=SLOT_NAME,
                decode=False,
                options={'add-tables': 'public.customers', 'format-version': '2'}
            )
            logger.info(f"Started replication with slot {SLOT_NAME}")

            while True:
                msg = self.cur.read_message()
                if msg:
                    logger.debug(f"Received message with LSN {msg.data_start}")
                    self.handle_event(msg.payload)
                    self.cur.send_feedback(write_lsn=msg.data_start)
                else:
                    time.sleep(0.1)
        except psycopg2.OperationalError as e:
            logger.error(f"Replication error: {e}. Reconnecting...")
            self.connect_postgres()
            self.start()
        except KeyboardInterrupt:
            logger.info("Stopping replication")
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            if self.rmq_conn:
                self.rmq_conn.close()

if __name__ == '__main__':
    consumer = CDCConsumer()
    consumer.start()
