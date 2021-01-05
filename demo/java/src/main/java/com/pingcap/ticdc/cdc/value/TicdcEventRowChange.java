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

import java.util.List;

public class TicdcEventRowChange extends TicdcEventBase {
    private String updateOrDelete; // should be "u" or "d"
    private List<TicdcEventColumn> oldColumns;
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

    public List<TicdcEventColumn> getOldColumns() {
        return oldColumns;
    }

    public void setOldColumns(List<TicdcEventColumn> oldColumns) {
        this.oldColumns = oldColumns;
    }
}
