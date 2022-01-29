# Intro

This component shows how to start the cluster and run some tests by docker and docker-compose.

## Build

To build this repo, you can simply run `./prepare.sh`. Make sure you have installed docker and have run the docker daemon.

## Deploy

There are several configure files to use. The file name suggests the number of running server-master and executor nodes. For example, `1m1e.yaml` means this file contains one server-master and one executor. Use `./deploy.sh 1m1e.yaml` to deploy cluster.

## Destroy

Use `./stop.sh 1m1e.yaml` to destroy the cluster.

## Client

`client.sh` is used to send command to server-master. For old workload, we can use `./client.sh submit-job --master-addr server-master:10240 --config ./cmd/master-client/bench-example.toml` to run the simple workload.
Note that although we can successfully send the job, the job can't run as expected right now because the chaos of normal address and advertise address. We should manage to prove that the new worker-master framework can launch successfully.
