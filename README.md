# Introduction

This repo aims to simulate a streaming workload and do a benchmark of the task scheduler.

## design

The task scheduler has some important struct and implements the interface:

- **scheduler**, the (singleton) instance organizes and executes tasks.
  - **Run** function that actually drives the execution of the tasks and is thread-safe.
  - *Queue* field that contains the runnable tasks.
- **task**, also called *taskContainer*, wraps the logic of operator and maintains the status and input/output channels.
  - **Poll** triggers a executing action of the task, which will return quickly. During the poll action, the task will
    - read inputs channels, if the channels are empty, return `blocked`. The read action fetches a batch of data.
  - *status* records the current status of task, including
    - `runnable` means the task is in the *Queue* and can run immediately.
    - `blocked` means the task is waiting someone to trigger, say output data consuming, input data arriving or i/o task completed.
    - `awaiting` means this task has been awoken by someone. If the **Poll** function ends and checks this status, we should put this task back to queue and reset the status to runnable.
- **Operator** actually implement the user logic. Every operator is contained by a task. Ideally, operators can construct an operating tree like a typical sql engine.
  - **prepare** is called when constructing the operator and prepare some resources such as workerpool or grpc stream.
  - **next([]\*records)** accepts the incoming data and returns the result data or blocked status.

## playbook

the workload is designed as three types of task

- receive task. receives records from a single grpc stream. Assuming the task number is N, it recieves data belonging to M tables and dispatches a record to one of the M tasks according to the table id in the record.
- hash task. do some simple computing task, fill the hash value in the record.
- sink task. write the record to a local file, and records the ending time.

### compile

execute `go build` in the root dir and `cd producer && go build` to build the producer which emit records continously.

### producer

producer is set to generate records repeatly. The default address is `127.0.0.1:50051`. After launching the producer, write the address to the `servers` in the config file. If you want to read from multiple grpc stream, just repeat the address in the `servers` array.

### config and launch the test

You can set the stream number by setting the `servers` address in the config file, and set the table number by setting the `tableNum` in the config file. Finally, set the `timeout` field which means the bench time.

to run the test

```[shell]
./microcosom -config demo.toml
```

After the timeout, we can get the analysis result, such as

```[log]
2021/10/11 00:19:51 cfg table num 10 addr 127.0.0.1:50051
2021/10/11 00:20:51 tid 0 qps 28427 avgLag 72 ms
2021/10/11 00:20:51 tid 1 qps 29543 avgLag 77 ms
2021/10/11 00:20:51 tid 2 qps 28658 avgLag 80 ms
2021/10/11 00:20:51 tid 3 qps 27868 avgLag 78 ms
2021/10/11 00:20:51 tid 4 qps 27039 avgLag 82 ms
2021/10/11 00:20:51 tid 5 qps 27662 avgLag 79 ms
2021/10/11 00:20:51 tid 6 qps 28533 avgLag 74 ms
2021/10/11 00:20:51 tid 7 qps 28347 avgLag 76 ms
2021/10/11 00:20:51 tid 8 qps 29186 avgLag 71 ms
2021/10/11 00:20:51 tid 9 qps 29281 avgLag 73 ms
```

we can get the qps and average lag of every table.