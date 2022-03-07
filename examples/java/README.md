# How to use

The following code shows how to parse ticdc data([ticdc open protocol](https://docs.pingcap.com/tidb/stable/ticdc-open-protocol)) which consumed from kafka.

```
TicdcEventFilter filter = new TicdcEventFilter();
for (KafkaMessage kafkaMessage : kafkaMessages) {
    TicdcEventDecoder ticdcEventDecoder = new TicdcEventDecoder(kafkaMessage);
    while (ticdcEventDecoder.hasNext()) {
        TicdcEventData data = ticdcEventDecoder.next();
        if (data.getTicdcEventValue() instanceof TicdcEventRowChange) {
            boolean ok = filter.check(data.getTicdcEventKey().getTbl(), data.getTicdcEventValue().getKafkaPartition(), data.getTicdcEventKey().getTs());
            if (ok) {
                // deal with row change event
            } else {
                // ignore duplicated messages
            }
        } else if (data.getTicdcEventValue() instanceof TicdcEventDDL) {
            // deal with ddl event
        } else if (data.getTicdcEventValue() instanceof TicdcEventResolve) {
            filter.resolveEvent(data.getTicdcEventValue().getKafkaPartition(), data.getTicdcEventKey().getTs());
            // deal with resolve event
        }
        System.out.println(JSON.toJSONString(data, true));
    }
}

```
[See com.pingcap.ticdc.cdc.TicdcEventDecoderTest.](src/test/java/com/pingcap/tiflow/cdc/TicdcEventDecoderTest.java).

# How to install
Prerequisites for building:

* Git
* Maven (we recommend version 3.2.5)
* Java 8

```
git clone git@github.com:pingcap/tiflow.git
cd ticdc/examples/java
mvn install
```

Now ticdc-decoder is installed. To add a dependency :

```xml
<dependency>
    <groupId>com.pingcap.ticdc.cdc</groupId>
    <artifactId>ticdc-decoder</artifactId>
    <version>4.0.6-SNAPSHOT</version>
</dependency>
``` 
