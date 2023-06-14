# MySQL-Kafka-Cassandra-DataPipeline

This repository focuses on the development of a data pipeline that simulates the streaming of data from a MySQL database to a Kafka topic. The data is then stored in a Cassandra database, allowing retrieval for subsequent analysis. A Python script is utilized to add records to the MySQL database.

## Project Architecture:

![MySQL-Kafka-Cassandra-DataPipeline-Architecture](https://github.com/adithyang64/MySQL-Kafka-Cassandra-DataPipeline/assets/67658457/86d5be86-63c7-48e0-8668-1c089d33668d)

## Tech Stack
- MySQL
- Kafka
- Python
- Cassandra

## Setup 

Create two Tables in the MySQL Table. The first table will store sample retail data, while the second table will function as a checkpoint table, storing the data of the most recently created record. This checkpoint data will aid in the incremental loading of data into the database.

Run the below Python Script to insert incremental data to the MySQL DB
```
InsertRecords_MySQL.py
```

Follow the necessary steps to create Kafka topics for the data streamed from a specific table in the MySQL database. And establish appropriate schema for the Kafka topics based on the columns present in the table.

Additionally, create a Cassandra database with a designated keyspace for storing the dumped data. Set up the required table within the Cassandra keyspace to prepare it for data storage.

Once above steps is completed, execute the below files

Run the Producer Code in Terminal 1, which streams the incremental data from MySQL DB
```
kafka_producer.py
```

Run the Consumer Code.py which reads data from the created Kafka Topic and dumps into the respective Table in a particular Keyspace of  Cassandra DB.
```
kafka_consumer-cassandra.py
```
