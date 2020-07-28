package org.pingcap.ticdc.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.pingcap.ticdc.cdc.key.TicdcEventKey;
import org.pingcap.ticdc.cdc.value.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TicdcEventDataReader implements Iterator<TicdcEventData> {
    private DataInputStream keyStream;
    private DataInputStream valueStream;
    private long version;

    private boolean hasNext = true;
    private long nextKeyLength;
    private long nextValueLength;

    private KafkaMessage kafkaMessage;

    // for test
    TicdcEventDataReader(byte[] keyBytes, byte[] valueBytes) {
        this(new KafkaMessage(keyBytes, valueBytes));
    }

    public TicdcEventDataReader(KafkaMessage kafkaMessage) {
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
        // 每一个 msg key 前 8 个字节是协议版本号
        // 目前协议版本号是写死的 1
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
        if (jsonObject.containsKey("u")) {
            updateOrDelete = "u";
        } else if (jsonObject.containsKey("d")) {
            updateOrDelete = "d";
        } else {
            // TODO 目前ticdc不区分insert，后续可能需要相应做改动
            throw new RuntimeException("Can not parse Value:" + json);
        }
        JSONObject row = jsonObject.getJSONObject(updateOrDelete);
        TicdcEventRowChange v = new TicdcEventRowChange(kafkaMessage);
        if (v.getType() == TicdcEventType.rowChange) {
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
            v.setColumns(columns);
        }
        return v;
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
