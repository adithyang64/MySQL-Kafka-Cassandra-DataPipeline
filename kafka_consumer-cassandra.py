import argparse
import pymongo
from pymongo import MongoClient
import json
import pandas as pd

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# Cassandra connection details
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

cloud_config= {
         'secure_connect_bundle': 'secure-connect-cassandra-learn.zip'
}
auth_provider = PlainTextAuthProvider('iOYPjsxWBTZwFtLMUeGhIrGQ', 'GM17I2F-.Zs.ZUrAYnHPpQKt4lyqAiRagsZ7GjTQT-iWblDnMeL6h6TJx1aiqNfGz6NI59T_Os-9W1Y5-UJfklvEizZJrpZ4sKuUSdL+9G-ii_3nglIndRvjPl_-JEtM')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

try:
    query = "use retail_data"
    session.execute(query)
    print("Inside the keyspace")
except Exception as err:
    print("Exception Occured while using Keyspace : ",err)

API_KEY = 'SXGOWFEE4QNXVIM7'
ENDPOINT_SCHEMA_URL  = 'https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud'
API_SECRET_KEY = '0L0Yr/8WLD1g5m+vfrXhaTdRTcBEohme0e4vIXSJ+IIUiN0ogzcNOL10bi9JZ5g+'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MECHANISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'NKJN6WO54OKBJGT5'
SCHEMA_REGISTRY_API_SECRET = 'iIn1HvAv1+PG/MB2EKXSKlTkNxVYLn2O+RqguRSYtrX0PBH0uYreWtqhjrRuai2P'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MECHANISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"

def main(topic):

    # To Get (Latest) Schema to be read from Kafka
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str = schema_registry_client.get_latest_version('topic_retail_data-value').schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Order.dict_to_order)

    

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "latest"
                     })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    messages = []
    main_record =[]

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    count = 0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                if len(messages) != 0:
                    messages = []
                continue

            messages = []
            
            order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))


            if order is not None:
                count=count+1

                # print("User record {}: order: {}\n"
                #      .format(msg.key(), order))
                messages.append(order.record)
                df = pd.DataFrame.from_records(messages)
                json_str = df.to_dict(orient='records')
                ######################## collection.insert_many(json_str) ####################
  
                # Prepare the INSERT statement
                # insert_query = session.prepare('''
                #     INSERT INTO data_retail (id, product_name, price, quantity, customer_name, ordered_at)
                #     VALUES (?, ?, ?, ?, ?, ?)
                # ''')

                # Extract values from the dictionary
                record = json_str[0]
                id_value = str(record['id'])
                product_name = str(record['product_name'])
                price = str(record['price'])
                quantity = str(record['quantity'])
                customer_name = str(record['customer_name'])
                ordered_at = str(record['ordered_at'])

                
                stmt = ("INSERT INTO test.data(id, product_name, price, quantity, customer_name, ordered_at) VALUES (%s, %s, %s, %s, %s, %s)")
                batch.add(stmt, [id_value, product_name, price, quantity,customer_name,ordered_at])
                results = session.execute(batch)
                print(results)
                #session.execute(insert_query, (id_value, product_name,price,quantity,customer_name, ordered_at))
                #session.execute("INSERT INTO data_retail (id, product_name, price, quantity, customer_name, ordered_at) VALUES (?, ?, ?, ?, ?, ?)", (id_value, product_name,price,quantity,customer_name, ordered_at))
                
                # for item in json_str:
                    # session.execute(insert_query, (item['id'], item['product_name'], item['price'], item['quantity'], item['customer_name'], item['ordered_at']))
                print(f"Record written to Cassandra: {json_str}")

        except KeyboardInterrupt:
            print(count)
            break

    consumer.close()

    

# Close the session and cluster
session.shutdown()
cluster.shutdown()

main("topic_retail_data")







# # Define the query to insert JSON data
# query = "INSERT INTO data_retail (id, json_data) VALUES (?, ?)"

# # Prepare the query for execution
# query = session.prepare(query)

# # Convert the dictionary to a JSON string
# json_data = json.dumps(json_str)

# # Execute the prepared query with the JSON data
# session.execute(query, (1, json_data))  # Assuming the 'id' column is of type int