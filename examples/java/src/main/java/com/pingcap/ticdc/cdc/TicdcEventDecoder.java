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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.ticdc.cdc.key.TicdcEventKey;
import com.pingcap.ticdc.cdc.value.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Ticdc open protocol message decoder, parse one kafka message into multiple TicdcEventData instances.
 * <pre>
 * Example:
 * TicdcEventFilter filter = new TicdcEventFilter();
 * for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
 *     TicdcEventDecoder ticdcEventDecoder = new TicdcEventDecoder(kafkaMessage);
 *     while (ticdcEventDecoder.hasNext()) {
 *         TicdcEventData data = ticdcEventDecoder.next();
 *         if (data.getTicdcEventValue() instanceof TicdcEventRowChange) {
 *             boolean ok = filter.check(data.getTicdcEventKey().getTbl(), data.getTicdcEventValue().getKafkaPartition(), data.getTicdcEventKey().getTs());
 *             if (ok) {
 *                 // deal with row change event
 *             } else {
 *                 // ignore duplicated messages
 *             }
 *         } else if (data.getTicdcEventValue() instanceof TicdcEventDDL) {
 *             // deal with ddl event
 *         } else if (data.getTicdcEventValue() instanceof TicdcEventResolve) {
 *             filter.resolveEvent(data.getTicdcEventValue().getKafkaPartition(), data.getTicdcEventKey().getTs());
 *             // deal with resolve event
 *         }
 *     }
 * }
 * </pre>
 */
public class TicdcEventDecoder implements Iterator<TicdcEventData> {

    private static final String UPDATE_NEW_VALUE_TOKEN = "u";
    private static final String UPDATE_OLD_VALUE_TOKEN = "p";

    private DataInputStream keyStream;
    private DataInputStream valueStream;
    private long version;

    private boolean hasNext = true;
    private long nextKeyLength;
    private long nextValueLength;

    private KafkaMessage kafkaMessage;

    // visible for test
    TicdcEventDecoder(byte[] keyBytes, byte[] valueBytes) {
        this(new KafkaMessage(keyBytes, valueBytes));
    }

    public TicdcEventDecoder(KafkaMessage kafkaMessage) {
        this.kafkaMessage = kafkaMessage;
        keyStream = new DataInputStream(new ByteArrayInputStream(kafkaMessage.getKey()));
        readKeyVersion();
        readKeyLength();
        valueStream = new DataInputStream(new ByteArrayInputStream(kafkaMessage.getValue()));
        readValueLength();
    }

    private void readKeyLength() {
        try {
            nextKeyLength = keyStream.readLong();
            hasNext = true;
        } catch (EOFException e) {
            hasNext = false;
        } catch (Exception e) {
            throw new RuntimeException("Illegal format, can not read length", e);
        }
    }

    private void readValueLength() {
        try {
            nextValueLength = valueStream.readLong();
        } catch (EOFException e) {
            // ignore
        } catch (Exception e) {
            throw new RuntimeException("Illegal format, can not read length", e);
        }
    }


    private void readKeyVersion() {
        try {
            version = keyStream.readLong();
        } catch (IOException e) {
            throw new RuntimeException("Illegal format, can not read version", e);
        }
        if (version != 1) {
            throw new RuntimeException("Illegal version, should be 1");
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }


    public TicdcEventKey createTidbEventKey(String json) {
        JSONObject jsonObject = JSON.parseObject(json);
        TicdcEventKey key = new TicdcEventKey();
        key.setTs(jsonObject.getLongValue("ts"));
        key.setT(jsonObject.getLongValue("t"));
        key.setScm(jsonObject.getString("scm"));
        key.setTbl(jsonObject.getString("tbl"));
        return key;
    }

    public TicdcEventBase createTidbEventValue(String json) {
        // resolve
        if (json == null || json.length() == 0) {
            return new TicdcEventResolve(kafkaMessage);
        }
        JSONObject jsonObject = JSON.parseObject(json);

        // ddl
        if (jsonObject.containsKey("q")) {
            TicdcEventDDL ddl = new TicdcEventDDL(kafkaMessage);
            ddl.setQ(jsonObject.getString("q"));
            ddl.setT(jsonObject.getIntValue("t"));
            return ddl;
        }

        // row change
        String updateOrDelete;
        if (jsonObject.containsKey(UPDATE_NEW_VALUE_TOKEN)) {
            updateOrDelete = UPDATE_NEW_VALUE_TOKEN;
        } else if (jsonObject.containsKey("d")) {
            updateOrDelete = "d";
        } else {
            throw new RuntimeException("Can not parse Value:" + json);
        }

        JSONObject row = jsonObject.getJSONObject(updateOrDelete);
        TicdcEventRowChange v = new TicdcEventRowChange(kafkaMessage);
        v.setUpdateOrDelete(updateOrDelete);
        if (v.getType() == TicdcEventType.rowChange) {
            List<TicdcEventColumn> columns = getTicdcEventColumns(row);
            v.setColumns(columns);
        }

        if(UPDATE_NEW_VALUE_TOKEN.equals(updateOrDelete) ){
            row = jsonObject.getJSONObject(UPDATE_OLD_VALUE_TOKEN);
            if(row != null){
                v.setOldColumns(getTicdcEventColumns(row));
            }
        }

        return v;
    }
    private List<TicdcEventColumn> getTicdcEventColumns(JSONObject row) {
        List<TicdcEventColumn> columns = new ArrayList<>();
        if (row != null) {
            for (String col : row.keySet()) {
                JSONObject columnObj = row.getJSONObject(col);
                TicdcEventColumn column = new TicdcEventColumn();
                column.setH(columnObj.getBooleanValue("h"));
                column.setT(columnObj.getIntValue("t"));
                column.setV(columnObj.get("v"));
                column.setName(col);
                columns.add(column);
            }
        }
        return columns;
    }
    @Override
    public TicdcEventData next() {
        try {
            byte[] key = new byte[(int) nextKeyLength];
            keyStream.readFully(key);
            readKeyLength();
            String keyData = new String(key, StandardCharsets.UTF_8);
            TicdcEventKey ticdcEventKey = createTidbEventKey(keyData);

            byte[] val = new byte[(int) nextValueLength];
            valueStream.readFully(val);
            readValueLength();
            String valueData = new String(val, StandardCharsets.UTF_8);
            return new TicdcEventData(ticdcEventKey, createTidbEventValue(valueData));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
