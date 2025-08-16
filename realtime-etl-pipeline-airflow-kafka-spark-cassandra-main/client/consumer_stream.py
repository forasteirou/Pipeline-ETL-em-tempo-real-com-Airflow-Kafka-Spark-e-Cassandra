# Imports the sys module for argument handling and system interaction
import sys

# Imports the logging module for log recording
import logging

# Imports the json module for working with JSON data
import json

# Imports the re module for working with regular expressions
import re

# Imports the argparse module for command-line argument handling
import argparse

# Imports the Cluster class from Cassandra to create database connections
from cassandra.cluster import Cluster

# Imports SparkSession to create Spark sessions
from pyspark.sql import SparkSession

# Imports PySpark functions for data processing with Spark
from pyspark.sql.functions import from_json, col, instr

# Imports structured data types from PySpark
from pyspark.sql.types import StructType, StructField, StringType

# Configures the logger to display log messages with INFO level
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Function to create a keyspace in Cassandra
def cria_keyspace(session):

    # Executes the statement using the provided session
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS dados_usuarios
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )

    print("Keyspace created successfully!")

# Function to create a table in Cassandra
def cria_tabela(session):

    # Executes the statement using the provided session
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS dados_usuarios.tb_usuarios (
            id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """
    )

    print("Table created successfully!")

# Function to format strings before inserting them into Cassandra
def formata_string_cassandra(text: str):
    return re.sub(r"'", r"''", text)

# Function to insert formatted data into Cassandra
def insere_dados(session, row):

    # Formats and extracts fields from the received record
    user_id         = formata_string_cassandra(row.id)
    first_name      = formata_string_cassandra(row.first_name)
    last_name       = formata_string_cassandra(row.last_name)
    email           = formata_string_cassandra(row.email)
    username        = formata_string_cassandra(row.username)
    gender          = formata_string_cassandra(row.gender)
    address         = formata_string_cassandra(row.address)
    post_code       = formata_string_cassandra(row.post_code)
    dob             = formata_string_cassandra(row.dob)
    registered_date = formata_string_cassandra(row.registered_date)
    phone           = formata_string_cassandra(row.phone)
    picture         = formata_string_cassandra(row.picture)

    try:
        # Creates the query
        query = f"""
            INSERT INTO dados_usuarios.tb_usuarios(
                id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture
            ) VALUES (
                '{user_id}', '{first_name}', '{last_name}', '{gender}', '{address}', '{post_code}', 
                '{email}', '{username}', '{dob}', '{registered_date}', '{phone}', '{picture}'
            )
        """
        
        # Inserts formatted data into the Cassandra table
        session.execute(query)
        logging.info(f"Log - Data inserted for record: {user_id} - {first_name} {last_name} - {email}")
    except Exception as e:
        # Displays error if data insertion fails
        logging.error(f"Log - Data could not be inserted due to error: {e}")
        print(f"Log - This is the query:\n{query}")

# Function to create a Spark connection
def cria_spark_connection():

    try:
        # Configures and creates the Spark connection
        s_conn = (
            SparkSession.builder.appName("Project Data Stream")
            .master("spark://spark-master:7077")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "cassandra")
            .config("spark.cassandra.connection.port", "9042")
            .config("spark.executor.memory", "1g")
            .config("spark.executor.cores", "1")
            .config("spark.cores.max", "2")
            .getOrCreate()
        )

        # Sets log level
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Log - Spark Connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Log - Could not create Spark Connection due to error: {e}")
        return None

# Function to create a Kafka connection in Spark
def cria_kafka_connection(spark_conn, stream_mode):

    try:
        # Configures and creates a Spark DataFrame to read data from Kafka
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "kafka_topic")
            .option("startingOffsets", stream_mode)
            .load()
        )
        logging.info("Log - Kafka DataFrame created successfully")
        return spark_df
    except Exception as e:
        # Displays a warning if Kafka DataFrame creation fails
        logging.warning(f"Log - Kafka DataFrame could not be created due to error: {e}")
        return None

# Function to create a structured DataFrame from Kafka data
def cria_df_from_kafka(spark_df):

    # Defines the schema of the data received in JSON format
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("dob", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )

    # Processes Kafka data to extract and filter records
    return (
        spark_df.selectExpr("CAST(value AS STRING)")            # Converts data to string
        .select(from_json(col("value"), schema).alias("data"))  # Parses JSON to structured columns
        .select("data.*")                                       # Extracts all fields from "data"
        .filter(instr(col("email"), "@") > 0)                   # Filters records where email contains '@'
    )

# Function to create a connection to Cassandra
def cria_cassandra_connection():

    try:
        # Creates a cluster and returns the Cassandra session
        cluster = Cluster(["cassandra"])
        return cluster.connect()
    except Exception as e:
        # Displays error if Cassandra connection fails
        logging.error(f"Log - Could not create Cassandra connection due to error: {e}")
        return None

# Main entry point of the program
if __name__ == "__main__":
    
    # Configures the parser for command-line arguments
    parser = argparse.ArgumentParser(description = "Real Time ETL.")
    
    # Adds the argument for data consumption mode
    parser.add_argument(
        "--mode",
        required=True,
        help="Data consumption mode",
        choices=["initial", "append"],
        default="append",
    )

    # Parses the provided arguments
    args = parser.parse_args()

    # Sets the stream mode based on the argument
    stream_mode = "earliest" if args.mode == "initial" else "latest"

    # Creates connections to Cassandra and Spark
    session = cria_cassandra_connection()
    spark_conn = cria_spark_connection()

    # If both sessions are created
    if session and spark_conn:

        # Creates keyspace and table in Cassandra
        cria_keyspace(session)
        cria_tabela(session)

        # Creates Kafka connection and retrieves DataFrame
        kafka_df = cria_kafka_connection(spark_conn, stream_mode)

        if kafka_df:

            # Creates structured DataFrame from Kafka data
            structured_df = cria_df_from_kafka(kafka_df)

            # Function to process data batches
            def process_batch(batch_df, batch_id):
                
                # Iterates over the batch rows and inserts data into Cassandra
                for row in batch_df.collect():
                    insere_dados(session, row)

            # Configures continuous processing of the structured DataFrame
            query = (
                structured_df.writeStream
                .foreachBatch(process_batch)  # Defines per-batch processing
                .start()                      # Starts the stream
            )
        
            # Waits for the stream to finish
            query.awaitTermination()