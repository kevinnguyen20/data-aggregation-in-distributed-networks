FROM flink:1.17.0-scala_2.12-java11
RUN apt-get update && apt-get install -y iproute2

# Create a directory storing jobs
RUN mkdir /root/jobs

# Create a job and copy it into the container
COPY flink-aggregation-java/target/flink-aggregation-java-1.0-SNAPSHOT.jar /root/jobs
