# Open the terminal or command prompt, navigate to the folder with the files, and run the command below to create the image:

docker build -t kafka-spark-cassandra-consumer .

# Run the command below to create the container:

docker run --name client -dit --network server_data-net kafka-spark-cassandra-consumer

# Inside the container, run the command below to start the Kafka Consumer:

python consumer_stream.py --mode initial

# Open the terminal or command prompt and use the commands below to access Cassandra and check the storage result:

docker exec -it cassandra cqlsh
USE dados_usuarios;
SELECT * FROM tb_usuarios;

# Note: Uncomment the last line of the Dockerfile and recreate the image and container to automate the process.