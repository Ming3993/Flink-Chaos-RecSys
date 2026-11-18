FROM flink:1.19.1-scala_2.12-java11

USER root

# Cài đặt Python 3, pip và build tools tối thiểu cho PyFlink
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt PyFlink khớp đúng phiên bản image (1.19.1) và Redis client
RUN pip3 install --no-cache-dir apache-flink==1.19.1 redis==5.0.8

# Tải sẵn Kafka Connector JAR vào thư mục /opt/flink/lib để Flink tự động nhận diện
RUN curl -L -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

USER flink
