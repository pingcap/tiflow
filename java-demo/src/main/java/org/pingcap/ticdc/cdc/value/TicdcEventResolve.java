package org.pingcap.ticdc.cdc.value;


import org.pingcap.ticdc.cdc.KafkaMessage;

public class TicdcEventResolve extends TicdcEventBase {

    public TicdcEventResolve(KafkaMessage kafkaMessage) {
        super(TicdcEventType.resolved, kafkaMessage);
    }
}
