# Canal-JSON Handle Key Only Consume Example

This example demonstrates how to consume canal-json encoded handle-key only message.

You can learn more about it from [Handle messages that exceed the Kafka topic limit](https://docs.pingcap.com/tidb/dev/ticdc-sink-to-kafka#handle-messages-that-exceed-the-kafka-topic-limit).

## How to build

This sample code assumes the following environment:

1. The kafka address is `127.0.0.1:9092`
3. The kafka topic is `canal-json-handle-key-only-example`
4. The consumer group id is `canal-json-handle-key-only-example-group`

You can modify all these default values, to match your environment. 

Make sure [Golang](https://go.dev/) is installed on your development machine. Build the executable file by the following command:

```shell
go mod tidy

go build main.go
```

## How to use

1. Deploy a local Kafka cluster. You can learn about it from [Apache Kafka Quickstart](https://kafka.apache.org/quickstart)
2. Deploy a TiCDC cluster and create a kafka changefeed using canal-json protocol and enable the handle-key-only functionality, make sure the `max-message-byte` is set less than the encoded message size, the default value is 1MB, you can write row event larger than 1MB to make it work.
3. Create one Table and write some data in the TiDB, to make the changefeed produce data to the kafka topic.
4. Run the previous build executable consumer program, and you will see the data consumed from the kafka topic.
