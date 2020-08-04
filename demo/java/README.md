### Ticdc java parser demo

The following code shows how to parse ticdc data([ticdc open protocol](https://docs.pingcap.com/tidb/stable/ticdc-open-protocol)) which consumed from kafka.

```
 TicdcEventFilter filter = new TicdcEventFilter();
 for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
     TicdcEventDataReader ticdcEventDataReader = new TicdcEventDataReader(kafkaMessage);
     while (ticdcEventDataReader.hasNext()) {
         TicdcEventData data = ticdcEventDataReader.next();
         if (data.getTicdcEventValue() instanceof TicdcEventRowChange) {
             // check for duplicated messages
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
[See com.pingcap.ticdc.cdc.TicdcEventDataReaderTest.](src/test/java/com/pingcap/ticdc/cdc/TicdcEventDataReaderTest.java).
