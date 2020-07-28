package org.pingcap.ticdc.cdc;

public class KafkaMessage {
    private byte[] key;
    private byte[] value;
    private int partition;
    private long offset;
    private long timestamp;

    public KafkaMessage() {
    }

    public KafkaMessage(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
