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

package com.pingcap.ticdc.cdc.value;

import com.pingcap.ticdc.cdc.KafkaMessage;

public class TicdcEventDDL extends TicdcEventBase {
    private String q; // DDL query SQL
    private int t; // DDL type code, see: https://docs.pingcap.com/tidb/stable/ticdc-open-protocol#ddl-type-code

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
