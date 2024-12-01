FROM spark:3.5.3-scala2.12-java11-ubuntu

USER root

RUN apt update && \
    apt install -y vim less zip unzip && \
    apt clean

RUN curl -s "https://get.sdkman.io" | bash && \
    bash -c "source /root/.sdkman/bin/sdkman-init.sh && sdk install sbt"

ENTRYPOINT ["/bin/bash"]
