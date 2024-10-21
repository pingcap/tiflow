FROM openjdk:17 as jdk_container

FROM hub.pingcap.net/jenkins/centos7_golang-1.23:latest
RUN curl https://archive.apache.org/dist/pulsar/pulsar-3.2.0/apache-pulsar-3.2.0-bin.tar.gz -o pulsar.tar.gz && \
    tar -xvf pulsar.tar.gz && \
    mv apache-pulsar-3.2.0 pulsar && \
    rm pulsar.tar.gz
USER root
RUN yum install -y nc
RUN mv pulsar /usr/local
USER jenkins
COPY --from=jdk_container  /usr/java /usr/java
ENV PATH="/usr/java/openjdk-17/bin:/usr/local/pulsar/bin:${PATH}"

