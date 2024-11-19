# Debezium Integration Test Locally

This file shows how to run debezium integration test locally

```
cd tiflow/tests/integration_tests/debezium
docker compose up
```

```
curl -i -X POST \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  localhost:8083/connectors/ --data-binary @- << EOF
{
  "name": "my-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "topic.prefix": "default",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schemahistory.test",
    "transforms": "x",
    "transforms.x.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.x.regex": "(.*)",
    "transforms.x.replacement":"output_debezium",
    "binary.handling.mode": "base64",
    "decimal.handling.mode": "double"
  }
}
EOF
```

```
tiup playground nightly --tiflash 0 --ticdc 1 
```

```
tiup cdc cli changefeed create -c test \
  --server=http://127.0.0.1:8300 --config changefeed.toml \
  --sink-uri="kafka://127.0.0.1:9094/output_ticdc?protocol=debezium"
```

```
go run ./src --db.mysql="root@tcp(127.0.0.1:3306)/{db}?allowNativePasswords=true" --cdc.kafka 127.0.0.1:9094
```


