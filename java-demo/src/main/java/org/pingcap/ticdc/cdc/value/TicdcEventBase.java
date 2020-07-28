package org.pingcap.ticdc.cdc.value;


import org.pingcap.ticdc.cdc.KafkaMessage;

public class TicdcEventBase {
    private TicdcEventType type;
    private int kafkaPartition;
    private long kafkaOffset;
    private long kafkaTimestamp;

    public TicdcEventBase() {
    }

    public TicdcEventBase(TicdcEventType type, KafkaMessage kafkaMessage) {
        this.type = type;
        this.kafkaPartition = kafkaMessage.getPartition();
        this.kafkaOffset = kafkaMessage.getOffset();
        this.kafkaTimestamp = kafkaMessage.getTimestamp();
    }

    public TicdcEventType getType() {
        return type;
    }

    public void setType(TicdcEventType type) {
        this.type = type;
    }

    public int getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    public long getKafkaTimestamp() {
        return kafkaTimestamp;
    }

    public void setKafkaTimestamp(long kafkaTimestamp) {
        this.kafkaTimestamp = kafkaTimestamp;
    }
}
