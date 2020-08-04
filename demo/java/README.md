### Ticdc java parser demo

The following code shows how to parse ticdc data([ticdc open protocol](https://docs.pingcap.com/tidb/stable/ticdc-open-protocol)) which consumed from kafka.

```
 TicdcEventDataReader ticdcEventDataReader = new TicdcEventDataReader(kafkaMessageKey, kafkaMessageValue);
            while (ticdcEventDataReader.hasNext()) {
                TicdcEventData data = ticdcEventDataReader.next();
                if(data.getTicdcEventValue() instanceof TicdcEventRowChange) {
                    // deal with row change event``
                } else if(data.getTicdcEventValue() instanceof TicdcEventDDL) {
                    // deal with ddl event
                } else if(data.getTicdcEventValue() instanceof TicdcEventResolve) {
                    // deal with resolve event
                }
            }
```
[See com.pingcap.ticdc.cdc.TicdcEventDataReaderTest.](src/test/java/com/pingcap/ticdc/cdc/TicdcEventDataReaderTest.java).
