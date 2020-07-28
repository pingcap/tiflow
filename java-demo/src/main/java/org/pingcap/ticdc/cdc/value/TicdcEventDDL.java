package org.pingcap.ticdc.cdc.value;


import org.pingcap.ticdc.cdc.KafkaMessage;

/**
 * https://docs.pingcap.com/zh/tidb/v4.0/ticdc-open-protocol#column-%E5%92%8C-ddl-%E7%9A%84%E7%B1%BB%E5%9E%8B%E7%A0%81
 */
public class TicdcEventDDL extends TicdcEventBase {
    private String q; //DDL Query SQL
    private int t; //DDL 类型，详见：Column 和 DDL 的类型码

    public TicdcEventDDL(KafkaMessage kafkaMessage) {
        super(TicdcEventType.ddl, kafkaMessage);
    }

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public int getT() {
        return t;
    }

    public void setT(int t) {
        this.t = t;
    }
}
