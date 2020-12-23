# Background

- [Apache Avro](https://avro.apache.org/) is a data serialization system, which provides algorithms to convert complex data structures to and from compact binary representations. The Avro format is compact in the sense that it is *not* self-explanatory the way JSON is, and ideally the schema of the data should be acquired separately from the data, and therefore a centralized Schema Registry could be ideal in some cases. 

- [Kafka Connect](https://docs.confluent.io/current/connect/index.html) is a component of the Kafka platform that aims to provide out-of-the-box integration of Kafka with other data sources & sinks, especially RDBMSes such as Mysql, Postgresql and many others. Kafka Connect has out-of-the-box support to use Avro as the wire format of each Kafka message. To solve the aforementioned schema problem, Kafka Connect ships with **Confluent Schema Registry**, which, through RESTful APIs, provides Kafka itself as well as other applications ability to acquire and share schemas of data that are to be transmitted in the Avro format.

# Feature
- TiCDC can now output data in Avro format to Kafka. It will automatically register the schema of the relevant table(s) to a user-managed Confluent Schema Registry instance, and the Avro data is compatible with the JDBC sink connector of Kafka Connect.
- User interface supports "avro" as a sink-uri parameter for Kafka sink, and accepts "registry=http://..." as a parameter in "--opts". For example `bin/cdc cli changefeed create --sink-uri "kafka://127.0.0.1:9092/testdb.test?protocol=avro" --opts registry="http://127.0.0.1:8081"`.

# Key Design Decisions
- Only the owner can update the Schema Registry.
- Processors retrieve the **latest** schema from the Registry.
- Processors maintain a local cache of the Avro schema(s) for the relevant tables. Cache items are invalidated if and only if the table's layout's `updateTs` has been changed.
- In order to maintain a stable interface and at the same time be compatible with Kafka Connect, the `AvroEventBatchEncoder` has a buffer that at most contains **one** pending message.

# Key Data Structures

### AvroEventBatchEncoder:
implements the interface `EventBatchEncoder`. 
##### Caveats
- `AppendResolvedEvent` is no-op because Kafka Connect does not expect such events.
- `AppendDDLEvent` *does not* emit any Kafka message but *does* update the Avro Schema Registry with the latest schema if necessary.
- `Size()` is always 0 or 1, which, albeit a slight violation of the expected semantics, clearly conveys whether the buffer is full or not.

### AvroSchemaManager
Provides basic operations on the Schema Registry. 
Note
 - `NewAvroSchemaManager` takes a parameter `subjectSuffix string` because Kafka Connect is supposed to look up the schemas by names in the forms of `{subject}-key` or `{subject}-value`.
 - The `goavro.Codec` instance is cached to avoid parsing the JSON representation of the Avro schemas each time.
 -  `AvroSchemaManager` is tested with the help of `jarcoal/httpmock`, which intercepts requests sent via the default HTTP client and mocks an implementation of the Schema Registry.
 
# Known Limitations
- The Kafka message keys are the internal `rowid`, which is not very useful to users.
- Given that a changefeed can only write to one Kafka topic, capturing on multiple tables could confuse Kafka sink connectors. 
