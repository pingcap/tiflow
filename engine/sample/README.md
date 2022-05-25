# Intro

This component shows how to start the cluster and run some tests by docker and docker-compose.

## Build

To build this repo, you can simply run `./prepare.sh`. Make sure you have installed docker and have run the docker daemon.

If you have build the binary on your local Linux, you can try `./build_image_from_local.sh`. 

## Deploy

There are several configure files to use. The file name suggests the number of running server-master and executor nodes. For example, `1m1e.yaml` means this file contains one server-master and one executor. Use `./deploy.sh 1m1e.yaml` to deploy cluster.

## Destroy

Use `./stop.sh 1m1e.yaml` to destroy the cluster.

## Run Demo

sudo rm -rf /tmp/df/master
docker-compose -f ./3m3e.yaml -f ./demo.yaml up --force-recreate

## Use Master Client to Run Demo workload

./client.sh --master-addr server-master-0:10240 submit-job --job-type CVSDemo --job-config ./sample/config/demo.json

## Cleanup

sudo rm -rf /tmp/df/master