package org.pingcap.ticdc.cdc;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class TicdcEventDataReaderTest {
    @Test
    public void test() throws IOException {
        List<KafkaMessage> kafkaMessagesFromTestData = getKafkaMessagesFromTestData();
        for (KafkaMessage kafkaMessage : kafkaMessagesFromTestData) {
            TicdcEventDataReader ticdcEventDataReader = new TicdcEventDataReader(kafkaMessage.getKey(),
                    kafkaMessage.getValue());
            while (ticdcEventDataReader.hasNext()) {
                TicdcEventData data = ticdcEventDataReader.next();
                System.out.println(JSON.toJSONString(data, true));
            }
        }
    }

    /**
     * Mock Kafka messages
     */
    private List<KafkaMessage> getKafkaMessagesFromTestData() throws IOException {
        List<KafkaMessage> kafkaMessages = new ArrayList<>();

        File keyFolder = getClasspathFile("data/key");
        File[] keyFiles = keyFolder.listFiles();
        File valueFolder = getClasspathFile("data/value");
        File[] valueFiles = valueFolder.listFiles();

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
        return new File(url.getFile());
    }
}