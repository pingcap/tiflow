package org.pingcap.ticdc.cdc;

import java.util.HashMap;
import java.util.Map;

/**
 * 1.每个partition需要为每个表记录一个tbl-max-ts，小于这个值的为重复数据（基于row change event的TS）
 * 2.每个partition同时还要记录resolved-max-ts，小于这个值的为重复数据（基于resolved event的TS）
 * 3.以上两部分逻辑结合，理论上就能做到完全避免重复
 *
 * @NotThreadSafe
 */
public class TicdcEventFilter {
    //    tablename_partition, rowChangeMaxTS
    private Map<String, Long> tableMaxTSMap = new HashMap<>();
    private Map<Integer, Long> resolveMaxTSMap = new HashMap<>();

    public boolean filter(long rowChangeTS, int partition, String tableName) {
        String mapKey = tableName + "_" + partition;
        Long rowChangeMaxTS = tableMaxTSMap.get(mapKey);
        if (rowChangeMaxTS == null) {
            tableMaxTSMap.put(mapKey, rowChangeMaxTS);
        } else {
            if (rowChangeTS < rowChangeMaxTS) {
                return false;
            }
            tableMaxTSMap.put(mapKey, rowChangeTS);
        }

        Long resolveMaxTS = resolveMaxTSMap.get(partition);
        if (rowChangeMaxTS != null) {
            if (rowChangeTS < resolveMaxTS) {
                return false;
            }
        }
        return true;
    }

    public void resolveEvent(int partition, long resolveTS) {
        resolveMaxTSMap.put(partition, resolveTS);
    }
}
