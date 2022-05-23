# Introduction

This repo implements a new prototype of distributed task scheduler.

# Build

## Prepare for building

**Go** version no less than 1.18

## Build

Please run `make tools_setup` to build tools

Simply run `make` to compile.

# Develop

## Prepare

**protoc** version no less than 3.8.0

## Run Test

Use `make unit_test` to run unit test and integrated test.

## Contribute

Run `make dev` before submitting pr.

# Run it

## Master

Master is set to process the requests from outside and to schedule and dispatch tasks.

## Executor

Executor is the worker process to run the tasks.

## Master-Client

Master-Client is set to interact with master.

## Demo

TODO

# Deploy Demonstration

## Single Master and Single Executor on Two Nodes

### Start Master on Single Node

```[shell]
./bin/master --config=./sample/config/master.toml --master-addr 0.0.0.0:10240 --advertise-addr ${ip0}:10240
```

Replace **ip0** with your advertising ip.

### Start Executor

```[shell]
./bin/executor --config=./sample/config/executor.toml --join ${ip0}:10240 --worker-addr 0.0.0.0:10241 --advertise-addr ${ip1}:10241
```

Replace **ip1** with your advertising executor ip.

## Three Master and One Executor on Three Nodes

Scaling out executor is quite simple. Right now we only support static config of masters, without scaling in and out dynamically.

In this case, we assume three node ips are ip0, ip1 and ip2. Replace them with real ip or hostname when operating.

### Start Master on Node (ip0)

```[shell]
./bin/master --name=ip0 --config=./sample/config/master.toml --master-addr 0.0.0.0:10240 --advertise-addr http://${ip0}:10240 --peer-urls 0.0.0.0:8291 --advertise-peer-urls http://${ip0}:8291 --initial-cluster ip0=http://${ip0}:8291,ip1=http://${ip1}:8291,ip2=http://${ip2}:8291
```

Note that --name can be any names, but has to be different from other two masters.

Deploying masters for ip1 and ip2 are similar.

### Start Executor on Node (ip0)

```[shell]
./bin/executor --config=./sample/config/executor.toml --join ${ip0}:10240,${ip1}:10240,${ip2}:10240 --worker-addr 0.0.0.0:10241 --advertise-addr ${ip0}:10241
```
