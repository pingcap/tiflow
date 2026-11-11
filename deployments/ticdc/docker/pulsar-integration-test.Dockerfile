FROM openjdk:17 as jdk_container

FROM golang:1.27.3 as go_toolchain

FROM ghcr.io/pingcap-qe/ci/base:v2025.11.9-1-ga8991fa-go1.25
RUN curl https://archive.apache.org/dist/pulsar/pulsar-3.2.0/apache-pulsar-3.2.0-bin.tar.gz -o pulsar.tar.gz && \
    tar -xvf pulsar.tar.gz && \
    mv apache-pulsar-3.2.0 pulsar && \
    rm pulsar.tar.gz
USER root
# Replace the base image's Go toolchain with the version required by go.mod.
RUN rm -rf /usr/local/go
COPY --from=go_toolchain /usr/local/go /usr/local/go
RUN yum install -y nc
RUN mv pulsar /usr/local
USER jenkins
COPY --from=jdk_container  /usr/java /usr/java
ENV PATH="/usr/java/openjdk-17/bin:/usr/local/pulsar/bin:${PATH}"

