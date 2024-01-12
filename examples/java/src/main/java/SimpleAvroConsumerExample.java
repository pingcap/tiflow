/*
 * Copyright 2023 PingCAP, Inc.
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleAvroConsumerExample {

    private static final Logger log = LoggerFactory.getLogger(SimpleAvroConsumerExample.class.getName());
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/schema.avsc"));

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-avro-consumer-example");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        String topic = "simple-avro-test";
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(record.value());
                    Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

                    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
                    GenericRecord result = datumReader.read(null, decoder);

                    String eventType = result.get("type").toString();
                    switch (eventType) {
                        case "WATERMARK":
                            log.info("watermark: {}", result.get("commitTs"));
                            break;
                        case "DDL":
                            log.info("ddl, ddl = {}", result.get("ddlQuery"));
                            break;
                        case "BOOTSTRAP":
                            log.info("bootstrap, tableSchema = {}", result.get("tableSchema"));
                            break;
                        default:
                            log.info("row changed event, type = {}, schema = {}, table = {}, commitTs = {}, buildTs = {}, schemaVersion = {}, claimCheckLocation = {}, handleKeyOnly = {}, ",
                                    eventType, result.get("schema"), result.get("table"), result.get("commitTs"), result.get("buildTs"), result.get("schemaVersion"), result.get("claimCheckLocation"), result.get("handleKeyOnly"));
                            if (result.hasField("checksum")) {
                                log.info("checksum = {}", result.get("checksum"));
                            }
                            if (result.hasField("data")) {
                                log.info("data = {}", result.get("data"));
                            }
                            if (result.hasField("old")) {
                                log.info("old = {}", result.get("old"));
                            }

                            break;
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
