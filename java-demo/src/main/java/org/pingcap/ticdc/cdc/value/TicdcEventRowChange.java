package org.pingcap.ticdc.cdc.value;


import org.pingcap.ticdc.cdc.KafkaMessage;

import java.util.List;

public class TicdcEventRowChange extends TicdcEventBase {
    // TidbEventType为rowChange时，此字段有意义
    private String updateOrDelete; //标识该 Event 是增加 Row 还是删除 Row，取值只可能是 "u"/"d"
    // TidbEventType为rowChange时，此字段有意义
    private List<TicdcEventColumn> columns;

    public TicdcEventRowChange(KafkaMessage kafkaMessage) {
        super(TicdcEventType.rowChange, kafkaMessage);
    }

    public String getUpdateOrDelete() {
        return updateOrDelete;
    }

    public void setUpdateOrDelete(String updateOrDelete) {
        this.updateOrDelete = updateOrDelete;
    }

    public List<TicdcEventColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<TicdcEventColumn> columns) {
        this.columns = columns;
    }
}
