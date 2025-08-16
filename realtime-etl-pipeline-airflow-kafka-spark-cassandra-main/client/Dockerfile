# Base image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jre \
    curl && \
    apt-get clean

# Install Python dependencies
RUN pip install pyspark==3.5.4 kafka-python==2.0.2 cassandra-driver==3.29.2

# Copy the script to the container
COPY consumer_stream.py /app/consumer_stream.py

# Set working directory
WORKDIR /app

# Default entry command
#CMD ["python", "consumer_stream.py", "--mode", "append"]