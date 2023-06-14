import argparse
from uuid import uuid4
#from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
# from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List
import mysql.connector
import datetime


columns = ['id', 'product_name', 'price', 'quantity', 'customer_name', 'ordered_at']

API_KEY = ''
ENDPOINT_SCHEMA_URL  = 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = ''
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MECHANISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = ''
SCHEMA_REGISTRY_API_SECRET = ''

# MySQL Connection Configuration
mysql_host = '127.0.0.1'
mysql_user = 'root'
mysql_password = ''
mysql_db = 'classicmodels'
mysql_table = 'retail_data'
checkpoint_table = 'retail_data_checkpoint_tbl'

def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MECHANISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Order:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_order(data: dict, ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"
    

# MySQL Connection
mysql_conn = mysql.connector.connect(host=mysql_host,
                             user=mysql_user,
                             password=mysql_password,
                             db=mysql_db)


def get_order_instance():
    try:
      # Read the last processed checkpoint value
      with mysql_conn.cursor() as cursor:
        cursor.execute(f"SELECT last_processed_ordered_at FROM {checkpoint_table}")
        result = cursor.fetchone()
        last_processed_created_at = result[0] if result else None
        print(last_processed_created_at)

      with mysql_conn.cursor() as cursor:
        # Get the last processed created_at value from Kafka (if any)
        #last_processed_created_at = "2023-06-11 01:11:10"  # Replace with logic to retrieve the last processed created_at value
        #last_processed_created_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Build the SQL query to fetch incremental data based on last processed created_at
        #sql_query = f"SELECT * FROM {mysql_table}"
        sql_query = f"SELECT * FROM {mysql_table} WHERE ordered_at > '{last_processed_created_at}'"
        print(sql_query)

        # Execute the SQL query
        cursor.execute(sql_query)

         # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Iterate over the rows and convert them to Order objects
        orders: List[Order] = []
        for row in rows:
            order_data = {
                'id': row[0],
                'product_name': row[1],
                'price': float(row[2]),
                'quantity': row[3],
                'customer_name': row[4],
                'ordered_at': row[5].strftime('%Y-%m-%d %H:%M:%S')
            }
            last_processed_ordered_at = order_data['ordered_at']
            order = Order(dict(zip(columns, order_data.values())))
            orders.append(order)
            yield order
        
        # Update the checkpoint value in the database
        with mysql_conn.cursor() as cursor:
          #cursor.execute(f"REPLACE INTO {checkpoint_table} (last_processed_ordered_at) VALUES (%s)", (last_processed_ordered_at,))
          cursor.execute("UPDATE retail_data_checkpoint_tbl SET last_processed_ordered_at = %s WHERE last_processed_ordered_at = %s", (last_processed_ordered_at, last_processed_created_at))
          mysql_conn.commit()

    finally:
      # Close the MySQL connection
      cursor.close()
      mysql_conn.close()


def case_to_dict(order: Order, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
        :param order:
    """

    # User._address must not be serialized; omit from dict
    return order.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):

    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "customer_name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "id": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "ordered_at": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "price": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "product_name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "quantity": {
      "description": "The type(v) type is used.",
      "type": "number"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    # To Get (Latest) Schema to be read from Kafka
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str = schema_registry_client.get_latest_version('topic_retail_data-value').schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, case_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    # while True:
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for order in get_order_instance():
            print(order)
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()), case_to_dict),
                             value=json_serializer(order, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()


main("topic_retail_data")
