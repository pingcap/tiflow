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
import com.pingcap.ticdc.cdc.value.TicdcEventDDL;
import com.pingcap.ticdc.cdc.value.TicdcEventResolve;
import com.pingcap.ticdc.cdc.value.TicdcEventRowChange;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class TicdcEventDecoderTest {
    @Test
    public void test() throws IOException {
        List<KafkaMessage> kafkaMessagesFromTestData = getKafkaMessagesFromTestData();
        TicdcEventFilter filter = new TicdcEventFilter();
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            TicdcEventDecoder ticdcEventDecoder = new TicdcEventDecoder(kafkaMessage);
            while (ticdcEventDecoder.hasNext()) {
                TicdcEventData data = ticdcEventDecoder.next();
                if (data.getTicdcEventValue() instanceof TicdcEventRowChange) {
                    boolean ok = filter.check(data.getTicdcEventKey().getTbl(), data.getTicdcEventValue().getKafkaPartition(), data.getTicdcEventKey().getTs());
                    if (ok) {
                        // deal with row change event
                    } else {
                        // ignore duplicated messages
                    }
                } else if (data.getTicdcEventValue() instanceof TicdcEventDDL) {
                    // deal with ddl event
                } else if (data.getTicdcEventValue() instanceof TicdcEventResolve) {
                    filter.resolveEvent(data.getTicdcEventValue().getKafkaPartition(), data.getTicdcEventKey().getTs());
                    // deal with resolve event
                }
                System.out.println(JSON.toJSONString(data, true));
            }
        }
    }

    /**
     * Mock Kafka messages, which usually consumed from kafka.
     */
    private List<KafkaMessage> getKafkaMessagesFromTestData() throws IOException {
        List<KafkaMessage> kafkaMessages = new ArrayList<>();

        File keyFolder = getClasspathFile("data/key");
        File[] keyFiles = keyFolder.listFiles();
        File valueFolder = getClasspathFile("data/value");
        File[] valueFiles = valueFolder.listFiles();
        Assert.assertNotNull(keyFiles);
        Assert.assertNotNull(valueFiles);
        Assert.assertEquals(keyFiles.length, valueFiles.length);

        for (int i = 0; i < keyFiles.length; i++) {
            File kf = keyFiles[i];
            byte[] kafkaMessageKey = Files.readAllBytes(kf.toPath());
//            System.out.printf("read key msg: %s\n", kf.toPath());

            File vf = valueFiles[i];
            byte[] kafkaMessageValue = Files.readAllBytes(vf.toPath());
//            System.out.printf("read value msg: %s\n", vf.toPath());
            KafkaMessage kafkaMessage = new KafkaMessage(kafkaMessageKey, kafkaMessageValue);
            kafkaMessage.setPartition(1);
            kafkaMessage.setOffset(1L);
            kafkaMessage.setTimestamp(System.currentTimeMillis());
            kafkaMessages.add(kafkaMessage);
        }
        return kafkaMessages;
    }

    private File getClasspathFile(String path) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource(path);
        Assert.assertNotNull(url);
        return new File(url.getFile());
    }
}