FROM apache/spark-py:latest

USER root

RUN apt update && \
    apt install -y vim less zip unzip && \
    apt clean

ENTRYPOINT ["/bin/bash"]
