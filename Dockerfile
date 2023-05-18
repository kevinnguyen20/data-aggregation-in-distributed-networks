FROM flink:1.17.0-scala_2.12-java11
RUN apt-get update && apt-get install -y iproute2
