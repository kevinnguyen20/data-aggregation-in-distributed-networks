FROM flink:1.12.7-scala_2.11
RUN apt-get update && apt-get install -y iproute2
