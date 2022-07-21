# dataflow engine

## Introduction

This repo implements a new prototype of distributed task scheduler.

### Master

Master is set to process the requests from outside and to schedule and dispatch tasks.

### Executor

Executor is the worker process to run the tasks.

### Master-Client

Master-Client is set to interact with master.

## Build

Simply run `make engine` to compile.

## Run Test

Use `make engine_unit_test` to run unit test and integrated test.

## Deploy Demonstration

### Single Master and Single Executor on Two Nodes

#### Start Master on Single Node

```[shell]
./bin/tiflow master --config=./engine/deployments/docker-compose/config/master.toml --master-addr 0.0.0.0:10240 --advertise-addr ${ip0}:10240 
```

Replace **ip0** with your advertising ip.

#### Start Executor

```[shell]
./bin/tiflow executor --config=./engine/deployments/docker-compose/config/executor.toml --join ${ip0}:10240 --worker-addr 0.0.0.0:10241 --advertise-addr ${ip1}:10241
```

Replace **ip1** with your advertising executor ip.

### Three Master and One Executor on Three Nodes

Scaling out executor is quite simple. Right now we only support static config of masters, without scaling in and out dynamically.

In this case, we assume three node ips are ip0, ip1 and ip2. Replace them with real ip or hostname when operating.

#### Start Master on Node (ip0)

```[shell]
./bin/tiflow master --name=ip0 --config=./engine/deployments/docker-compose/config/master.toml --master-addr 0.0.0.0:10240 --advertise-addr http://${ip0}:10240 --peer-urls 0.0.0.0:8291 --advertise-peer-urls http://${ip0}:8291 --initial-cluster ip0=http://${ip0}:8291,ip1=http://${ip1}:8291,ip2=http://${ip2}:8291
```

Note that --name can be any names, but has to be different from other two masters.

Deploying masters for ip1 and ip2 are similar.

#### Start Executor on Node (ip0)

```[shell]
./bin/tiflow executor --config=./engine/deployments/docker-compose/config/executor.toml --join ${ip0}:10240 --worker-addr 0.0.0.0:10241 --advertise-addr ${ip1}:10241
```

## Run engine in docker

This section shows how to start the cluster and run some tests by docker and docker-compose.

### Preparations

The following programs must be installed:

* docker
* docker-compose

Besides, make sure you have run the docker daemon. We recommend that you provide docker with at least 6+ cores and 8G+ memory. Of course, the more resources, the better.

Then, change directory to working directory:

```bash
cd ./deployments/docker-compose
```

### Build

To build this repo, you can run `../run-engine.sh build` in working directory (or simply run `make engine_image` in root directory of tiflow project).

If you have build the binary on your local Linux, you can try `../run-engine.sh build-local`.

### Deploy

There are several configure files to use. The file name suggests the number of running server-master and executor nodes. For example, `1m1e.yaml` means this file contains one server-master and one executor. Use `../run-engine.sh deploy 1m1e.yaml` to deploy cluster.

### Destroy

Use `../run-engine.sh stop 1m1e.yaml` to destroy the cluster.

### Cleanup

sudo rm -rf /tmp/df/master

## Run Demo

```bash
sudo rm -rf /tmp/df/master
../run-engine.sh deploy ./3m3e.yaml ./demo.yaml

cd ../../../ # root dir of tiflow
./bin/tiflow cli job create --master-addrs ${$server-master-ip0}:${port0} --job-type CVSDemo --job-config ./engine/deployments/docker-compose/config/demo.json
```

## Contribute
