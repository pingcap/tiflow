package org.pingcap.ticdc.cdc;

import org.pingcap.ticdc.cdc.key.TicdcEventKey;
import org.pingcap.ticdc.cdc.value.TicdcEventBase;

public class TicdcEventData {
    private TicdcEventKey ticdcEventKey;
    private TicdcEventBase tidbEventValue;

    public TicdcEventData() {
    }

    public TicdcEventData(TicdcEventKey ticdcEventKey, TicdcEventBase tidbEventValue) {
        this.ticdcEventKey = ticdcEventKey;
        this.tidbEventValue = tidbEventValue;
    }

    public TicdcEventKey getTicdcEventKey() {
        return ticdcEventKey;
    }

    public void setTicdcEventKey(TicdcEventKey ticdcEventKey) {
        this.ticdcEventKey = ticdcEventKey;
    }

    public TicdcEventBase getTidbEventValue() {
        return tidbEventValue;
    }

    public void setTidbEventValue(TicdcEventBase tidbEventValue) {
        this.tidbEventValue = tidbEventValue;
    }
}
