FROM ubuntu:22.04

# Install necessary packages
RUN apt-get update && \
    apt-get install -y build-essential librdkafka-dev libssl-dev libcjson-dev && \
    # Clear the cached package lists
    rm -rf /var/lib/apt/lists/*

# Create a directory in the container to copy files
WORKDIR /app

COPY continuousData.c cJSON.c cJSON.h delays.txt /app/
COPY cJSON.h /usr/include/cjson/cJSON.h

RUN gcc -o continuousData continuousData.c -lrdkafka -lcjson

# Set the entry point for the container
# ENTRYPOINT ["./continuousData"]
