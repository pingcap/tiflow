### Ticdc java parser demo

The following code shows how to parse ticdc data which consume from kafka.

```
 TicdcEventDataReader ticdcEventDataReader = new TicdcEventDataReader(kafkaMessageKey, kafkaMessageValue);
            while (ticdcEventDataReader.hasNext()) {
                TicdcEventData data = ticdcEventDataReader.next();
                System.out.println(JSON.toJSONString(data, true));
            }
```
