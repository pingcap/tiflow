# Avro checksum verification

This example demonstrates how to consume avro encoded data, and verify the checksum of the data.

You can learn more about it from [TiCDC Data Integrity Validation for Single-Row Data](https://docs.pingcap.com/tidb/dev/ticdc-integrity-check).

## How to build

This sample code assumes the following environment:

1. The kafka address is `127.0.0.1:9092`
2. Tha schema registry address is `http://127.0.0.1:8081`
3. The kafka topic is `avro-checksum-test`
4. The consumer group id is `avro-checksum-test`

You can modify all these default values, to match your environment. 

Make sure [Golang](https://go.dev/) is installed on your development machine. Build the executable file by the following command:

```shell
go mod tidy

go build main.go
```

## How to use

1. Deploy a local Kafka cluster and schema registry. You can learn about it from [Install Confluent Platform On-Premises](https://docs.confluent.io/platform/current/installation/overview.html#installation)
2. Deploy a TiCDC cluster and create a kafka changefeed using avro protocol and enable the checksum functionality. 
3. Create one Table and write some data in the TiDB, to make the changefeed produce data to the kafka topic.
4. Run the previous build executable consumer program, and you will see the data consumed from the kafka topic.
