FROM rockylinux:9.0 as builder

COPY ./bin/tiflow /tiflow
COPY ./bin/tiflow-demoserver /tiflow-demoserver
COPY ./bin/tiflow-chaos-case /tiflow-chaos-case
