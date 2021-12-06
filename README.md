# Introduction

This repo implements a new prototype of distributed task scheduler.

# Develop

## Build

Please run `make check` to build tools and format the code.

Simply run `make` to compile.

## Run Test

Use `make unit_test` to run unit test and integrated test.

# Run it 

## Master

Master is set to process the requests from outside and to schedule and dispatch tasks.

## Executor

Executor is the worker process to run the tasks. It receives `submit tasks` request from Master.

## Master-Client

Master-Client is set to interact with master. Right now it only supports `submit-job` command.

## Demo

the job of demo workload is devided to two parts:

- Producer
  - **Producer task** produces data continously. Every record has a table-id, ddl-version and a mark indicating whether it is a ddl record or data record. `Producer Task` shuffles records to different `Binlog task` to downstream side.
  - **Binlog task** serves as a server to provide recorder. It is only in charge to data transferring.
- Consumer
  - **receive task** receives records from a single grpc stream which connects to a **Binlog task**. It passes on data to `Syncer Task`.
  - **Syncer task** processes `DDL` record. It is supposed to wait all the other DDL with same version compeleted.
  - **Sink task** writes the record to a local file, and records the ending time.

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

#### submit a job

```[shell]
./bin/master-client submit-job --master-addr 127.0.0.1:10240 --config ./cmd/master-client/bench-example.toml
```

#### kill an executor

Then the tasks will be scheduled automatically.
