# Imports the `uuid` module to generate universally unique identifiers
import uuid

# Imports classes related to date and time
from datetime import datetime, timedelta

# Imports the main Airflow classes to create DAGs
from airflow import DAG

# Imports the Python operator to run Python functions as tasks
from airflow.operators.python import PythonOperator

# Defines the default arguments of the DAG, including the owner and start date
default_args = {"owner": "Project Data Stream",
                "start_date": datetime(2025, 1, 9, 8, 10)}

# Defines the function that fetches data from an API
def extrai_dados_api():

    # Imports the `requests` module to make HTTP requests
    import requests

    # Sends a GET request to retrieve data from a random user API
    res = requests.get("https://randomuser.me/api/")

    # Converts the response to JSON
    res = res.json()

    # Retrieves the first result from the JSON response
    res = res["results"][0]

    # Returns the retrieved data
    return res

# Defines the function that formats the data retrieved from the API
def formata_dados(res):

    # Creates an empty dictionary to store the formatted data
    data = {}

    # Gets the location data from the result
    location = res["location"]

    # Generates a unique ID for the user
    data["id"] = uuid.uuid4().hex

    # Extracts and stores the user's first name
    data["first_name"] = res["name"]["first"]

    # Extracts and stores the user's last name
    data["last_name"] = res["name"]["last"]

    # Extracts and stores the user's gender
    data["gender"] = res["gender"]

    # Formats and stores the user's full address
    data["address"] = (
        f"{str(location['street']['number'])} {location['street']['name']}, "
        f"{location['city']}, {location['state']}, {location['country']}"
    )

    # Stores the user's postal code
    data["post_code"] = location["postcode"]

    # Stores the user's email
    data["email"] = res["email"]

    # Stores the user's login username
    data["username"] = res["login"]["username"]

    # Stores the user's date of birth
    data["dob"] = res["dob"]["date"]

    # Stores the user's registration date
    data["registered_date"] = res["registered"]["date"]

    # Stores the user's phone number
    data["phone"] = res["phone"]

    # Stores the URL of the user's profile picture
    data["picture"] = res["picture"]["medium"]

    # Returns the formatted data
    return data

# Defines the function that streams data to Kafka
def stream_dados():

    # Imports the `json` module to handle JSON data
    import json

    # Imports the Kafka producer
    from kafka import KafkaProducer

    # Imports the `time` module to manage time intervals
    import time

    # Imports the `logging` module to log messages
    import logging

    try:

        # Creates a connection to the Kafka broker
        producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms=5000)

        # Waits 5 seconds before starting the stream
        time.sleep(5)

        # Logs the successful connection to the Kafka broker
        logging.info("Log - Kafka producer connected successfully.")

    except Exception as e:

        # Logs any error while trying to connect to the Kafka broker
        logging.error(f"Log - Failed to connect to the Kafka broker: {e}")
        return

    # Sets the initial time
    curr_time = time.time()

    # Runs the streaming loop for 60 seconds
    while True:

        # Checks if 60 seconds have passed
        if time.time() > curr_time + 60:  # 1 minute
            break
        try:

            # Retrieves data from the API
            res = extrai_dados_api()

            # Formats the data
            res = formata_dados(res)

            # Sends the data to the Kafka topic
            producer.send("data_kafka_topic", json.dumps(res).encode("utf-8"))

        except Exception as e:

            # Logs any error during the data streaming
            logging.error(f"Log - An error occurred: {e}")
            continue

# Defines the Airflow DAG
with DAG("real-time-etl-stack",
         # Sets the default arguments for the DAG
         default_args=default_args,
         # Sets the DAG schedule to once per day
         schedule=timedelta(days=1),
         # Prevents backfilling of missed runs
         catchup=False,
) as dag:
    # Defines the task that streams data
    streaming_task = PythonOperator(task_id="stream_from_api", 
                                    python_callable=stream_dados)