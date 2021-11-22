# Introduction

This repo implements a new prototype of distributed task scheduler.

# Concepts

## Job

## Task

## Operator

# Components

## Build

If this is the first time you build this repo, please run `cd tools && make`, which is to build the proto tools for further building.
Then execute `make` can build every components of the repo.

## Master

Master is set to process the requests from outside and to schedule and dispatch tasks.

## Executor

Executor executes the tasks.

## Master-Client

Master-Client is set to interact with master. Right now it only supports `submit-job` command.

## producer

producer mimics TiKV and generates records repeatly. The default address is `127.0.0.1:50051`. After launching the producer, write the address to the `servers` in the config file. If you want to read from multiple grpc stream, just repeat the address in the `servers` array.

## playbook

the workload is designed as three types of task

- receive task. receives records from a single grpc stream. Assuming the task number is N, it recieves data belonging to M tables and dispatches a record to one of the M tasks according to the table id in the record.
- hash task. do some simple computing task, fill the hash value in the record.
- sink task. write the record to a local file, and records the ending time.

### demonstration

#### Start Master

```[shell]
./bin/master --config ./cmd/master/example.toml
```

#### Start Executor 1, 2

```[shell]
./bin/executor --config ./cmd/executor/example.toml
```

change the `worker-addr` field in toml, and start another executor.

#### start producer

```[shell]
./bin/producer
```

#### submit a job

```[shell]
./bin/master-client submit-job --master-addr 127.0.0.1:10240 --config ./cmd/master-client/bench-example.toml
```

#### kill an executor

Then the tasks will be scheduled automatically.
