import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.Codec;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AvroChecksumVerification {

    private static final byte MAGIC_BYTE = 0;

    public static void main(String[] args) {
        String kafkaAddr = "127.0.0.1:9092";
        String schemaRegistryURL = "http://127.0.0.1:8081";
        String topic = "avro-checksum-test";
        String consumerGroupID = "avro-checksum-test";

        KafkaReader consumer = new KafkaReader(kafkaAddr, topic, consumerGroupID);
        consumer.open();

        while (true) {
            KafkaMessage message = consumer.fetchMessage();
            if (message == null) {
                break;
            }

            byte[] value = message.getValue();
            if (value.length == 0) {
                System.out.println("Delete event does not have value, skip checksum verification");
                continue;
            }

            Map<String, Object> valueMap;
            Map<String, Object> valueSchema;
            try {
                AvroResult avroResult = getValueMapAndSchema(value, schemaRegistryURL);
                valueMap = avroResult.getValueMap();
                valueSchema = avroResult.getValueSchema();
            } catch (Exception e) {
                System.err.println("Decode Kafka value failed: " + e.getMessage());
                break;
            }

            try {
                calculateAndVerifyChecksum(valueMap, valueSchema);
            } catch (Exception e) {
                System.err.println("Calculate checksum failed: " + e.getMessage());
                break;
            }

            if (!consumer.commitMessage(message)) {
                System.err.println("Commit Kafka message failed");
                break;
            }
        }

        consumer.close();
    }

    private static AvroResult getValueMapAndSchema(byte[] data, String url) throws IOException {
        int schemaID;
        byte[] binary;
        try {
            AvroExtractResult extractResult = extractSchemaIDAndBinaryData(data);
            schemaID = extractResult.getSchemaID();
            binary = extractResult.getBinary();
        } catch (Exception e) {
            throw new IOException(e);
        }

        Codec codec;
        try {
            codec = getSchema(url, schemaID);
        } catch (IOException e) {
            throw new IOException("Get schema failed: " + e.getMessage(), e);
        }

        GenericRecord nativeRecord;
        try {
            nativeRecord = codec.decode(binary);
        } catch (Exception e) {
            throw new IOException("Decode Avro record failed: " + e.getMessage(), e);
        }

        Map<String, Object> result = codec.getRecordAsMap(nativeRecord);
        Schema schema = nativeRecord.getSchema();
        Map<String, Object> schemaMap = AvroSchemaUtils.schemaToMap(schema);

        return new AvroResult(result, schemaMap);
    }

    private static AvroExtractResult extractSchemaIDAndBinaryData(byte[] data) throws IOException {
        if (data.length < 5) {
            throw new IOException("Invalid Avro data, length is less than 5");
        }
        if (data[0] != MAGIC_BYTE) {
            throw new IOException("Invalid Avro data, magic byte not found");
        }

        int schemaID = byteArrayToInt(data, 1);
        byte[] binary = new byte[data.length - 5];
        System.arraycopy(data, 5, binary, 0, data.length - 5);

        return new AvroExtractResult(schemaID, binary);
    }

    private static Codec getSchema(String url, int schemaID) throws IOException {
        String requestURI = url + "/schemas/ids/" + schemaID;
        HttpURLConnection connection = null;

        try {
            URL schemaURL = new URL(requestURI);
            connection = (HttpURLConnection) schemaURL.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json");

            int responseCode = connection.getResponseCode();
            if (responseCode != 200) {
                throw new IOException("Failed to query schema from the Registry, HTTP error: " + responseCode);
            }

            InputStream inputStream = connection.getInputStream();
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> responseMap = objectMapper.readValue(inputStream, Map.class);

            if (!responseMap.containsKey("schema")) {
                throw new IOException("Schema not found in Registry");
            }

            String schemaString = responseMap.get("schema").toString();
            Schema schema = new Schema.Parser().parse(schemaString);

            return new Codec(schema);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static void calculateAndVerifyChecksum(Map<String, Object> valueMap, Map<String, Object> valueSchema) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) valueSchema.get("fields");

        String expectedChecksum = (String) valueMap.get("_tidb_row_level_checksum");
        if (expectedChecksum == null || expectedChecksum.isEmpty()) {
            return; // No checksum, skip verification
        }

        long expected = Long.parseLong(expectedChecksum);
        long actual = calculateChecksum(fields, valueMap);

        if (expected != actual) {
            throw new IOException("Checksum mismatch: expected=" + expected + ", actual=" + actual);
        }

        System.out.println("Checksum verified: checksum=" + actual);
    }

    private static long calculateChecksum(Map<String, Object> fields, Map<String, Object> valueMap) {
        CRC32 crc32 = new CRC32();
        byte[] buf = new byte[0];

        for (String key : fields.keySet()) {
            if ("_tidb_op".equals(key)) {
                break;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> field = (Map<String, Object>) fields.get(key);
            String colName = (String) field.get("name");
            String tidbType = (String) field.get("type");
            String mysqlType = mysqlTypeFromTiDBType(tidbType);

            Object value = valueMap.get(colName);
            byte[] valueBytes = buildChecksumBytes(value, mysqlType);

            if (buf.length > 0) {
                buf = new byte[0];
            }

            buf = appendLengthValue(buf, valueBytes);

            crc32.update(buf);
        }

        return crc32.getValue();
    }

    private static String mysqlTypeFromTiDBType(String tidbType) {
        // Mapping of TiDB types to MySQL types
        switch (tidbType) {
            case "INT":
            case "INT UNSIGNED":
                return "INT";
            case "BIGINT":
            case "BIGINT UNSIGNED":
                return "BIGINT";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "BIT":
                return "BIT";
            case "DECIMAL":
                return "DECIMAL";
            case "TEXT":
                return "TEXT";
            case "BLOB":
                return "BLOB";
            case "ENUM":
                return "ENUM";
            case "SET":
                return "SET";
            case "JSON":
                return "JSON";
            case "DATE":
                return "DATE";
            case "DATETIME":
                return "DATETIME";
            case "TIMESTAMP":
                return "TIMESTAMP";
            case "TIME":
                return "TIME";
            case "YEAR":
                return "YEAR";
            default:
                throw new IllegalArgumentException("Unknown TiDB type: " + tidbType);
        }
    }

    private static byte[] buildChecksumBytes(Object value, String mysqlType) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        if (value == null) {
            return outputStream.toByteArray();
        }

        switch (mysqlType) {
            case "INT":
            case "BIGINT":
                long longValue = (Long) value;
                writeLong(outputStream, longValue);
                break;
            case "FLOAT":
                double doubleValue = (Double)((Float) value);
                writeDouble(outputStream, doubleValue);
                break;
            case "DOUBLE":
                double doubleValue2 = (Double) value;
                writeDouble(outputStream, doubleValue2);
                break;
            case "BIT":
                long bitValue = (Long) value;
                writeLong(outputStream, bitValue);
                break;
            case "DECIMAL":
                String decimalValue = (String) value;
                writeString(outputStream, decimalValue);
                break;
            case "TEXT":
            case "BLOB":
            case "SET":
            case "JSON":
            case "DATE":
            case "DATETIME":
            case "TIMESTAMP":
            case "TIME":
            case "YEAR":
                String stringValue = (String) value;
                writeString(outputStream, stringValue);
                break;
            case "ENUM":
                // TODO: Implement checksum calculation for ENUM, convert string to interger.
                throw new IllegalArgumentException("Checksum calculation for ENUM is not implemented");
            default:
                throw new IllegalArgumentException("Invalid type for checksum calculation: " + mysqlType);
        }

        return outputStream.toByteArray();
    }

    private static void writeLong(ByteArrayOutputStream outputStream, long value) {
        for (int i = 0; i < 8; i++) {
            outputStream.write((int) (value & 0xFF));
            value >>= 8;
        }
    }

    private static void writeFloat(ByteArrayOutputStream outputStream, float value) {
        int intValue = Float.floatToIntBits(value);
        writeInt(outputStream, intValue);
    }

    private static void writeDouble(ByteArrayOutputStream outputStream, double value) {
        long longValue = Double.doubleToLongBits(value);
        writeLong(outputStream, longValue);
    }

    private static void writeBytes(ByteArrayOutputStream outputStream, byte[] value) {
        for (byte b : value) {
            outputStream.write(b);
        }
    }

    private static void writeString(ByteArrayOutputStream outputStream, String value) {
        byte[] bytes = value.getBytes();
        writeInt(outputStream, bytes.length);
        writeBytes(outputStream, bytes);
    }

    private static byte[] appendLengthValue(byte[] buf, byte[] value) {
        int length = value.length;
        byte[] lengthBytes = new byte[4];
        lengthBytes[0] = (byte) length;
        lengthBytes[1] = (byte) (length >> 8);
        lengthBytes[2] = (byte) (length >> 16);
        lengthBytes[3] = (byte) (length >> 24);

        byte[] result = new byte[buf.length + lengthBytes.length + value.length];
        System.arraycopy(buf, 0, result, 0, buf.length);
        System.arraycopy(lengthBytes, 0, result, buf.length, lengthBytes.length);
        System.arraycopy(value, 0, result, buf.length + lengthBytes.length, value.length);

        return result;
    }

    // Helper methods for conversion from byte arrays to integers
    private static int byteArrayToInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    public static class AvroResult {
        private final Map<String, Object> valueMap;
        private final Map<String, Object> valueSchema;

        public AvroResult(Map<String, Object> valueMap, Map<String, Object> valueSchema) {
            this.valueMap = valueMap;
            this.valueSchema = valueSchema;
        }

        public Map<String, Object> getValueMap() {
            return valueMap;
        }

        public Map<String, Object> getValueSchema() {
            return valueSchema;
        }
    }

    public static class AvroExtractResult {
        private final int schemaID;
        private final byte[] binary;

        public AvroExtractResult(int schemaID, byte[] binary) {
            this.schemaID = schemaID;
            this.binary = binary;
        }

        public int getSchemaID() {
            return schemaID;
        }

        public byte[] getBinary() {
            return binary;
        }
    }

    public static class KafkaReader {
        private final String kafkaAddr;
        private final String topic;
        private final String groupID;

        public KafkaReader(String kafkaAddr, String topic, String groupID) {
            this.kafkaAddr = kafkaAddr;
            this.topic = topic;
            this.groupID = groupID;
        }

        public void open() {
            // Implementation for opening the Kafka reader
        }

        public KafkaMessage fetchMessage() {
            // Implementation for fetching a Kafka message
            return null; // Replace with actual message retrieval
        }

        public boolean commitMessage(KafkaMessage message) {
            // Implementation for committing a Kafka message
            return false; // Replace with actual commit logic
        }

        public void close() {
            // Implementation for closing the Kafka reader
        }
    }

    public static class KafkaMessage {
        private byte[] value;

        public KafkaMessage(byte[] value) {
            this.value = value;
        }

        public byte[] getValue() {
            return value;
        }
    }
}
