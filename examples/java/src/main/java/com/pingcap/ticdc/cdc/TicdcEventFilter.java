/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.ticdc.cdc;

import java.util.HashMap;
import java.util.Map;

/**
 * In most cases, the Row Changed Event of a version is sent only once,
 * but in special situations such as node failure and network partition,
 * the Row Changed Event of the same version might be sent multiple times.
 * <p>
 * This filter class can check duplicated row change events which produced under special situations.
 * </p>
 * You can just ignore this class if your application is idempotent or duplicated Row Change Event is acceptable.
 * See: https://docs.pingcap.com/tidb/stable/ticdc-open-protocol#restrictions
 *
 * @NotThreadSafe
 */
public class TicdcEventFilter {
    // tableName_partition -> Max TS from Row Change Event
    private Map<String, Long> tableMaxTSMap = new HashMap<>();
    // partition -> Max ts from Resolve Event
    private Map<Integer, Long> resolveMaxTSMap = new HashMap<>();

    /**
     * Check for duplicated row change event.
     *
     * @param tableName   Table name
     * @param partition   Kafka topic partition
     * @param rowChangeTS TS
     * @return Return false if the event is duplicated.
     */
    public boolean check(String tableName, int partition, long rowChangeTS) {
        String mapKey = tableName + "_" + partition;
        Long rowChangeMaxTS = tableMaxTSMap.get(mapKey);
        if (rowChangeMaxTS == null) {
            tableMaxTSMap.put(mapKey, rowChangeMaxTS);
        } else {
            if (rowChangeTS <= rowChangeMaxTS) {
                return false;
            }
            tableMaxTSMap.put(mapKey, rowChangeTS);
        }

        Long resolveMaxTS = resolveMaxTSMap.get(partition);
        if (rowChangeMaxTS != null) {
            if (rowChangeTS <= resolveMaxTS) {
                return false;
            }
        }
        return true;
    }

    /**
     * Record the max resolve TS when receive "Resolved Event"
     *
     * @param partition Kafka topic partition
     * @param resolveTS Resolve TS
     */
    public void resolveEvent(int partition, long resolveTS) {
        resolveMaxTSMap.put(partition, resolveTS);
    }
}
