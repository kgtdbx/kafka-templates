package producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import utility.ConfigurationUtility;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaAvroProducer {

    private static final String CONFIG_PATH = "src/main/resources/config.properties";

    public static void main(String[] args) {
        Configuration conf  = ConfigurationUtility.getConfiguration("src/main/resources/config.properties");
        File schemaFile = new File(conf.getString("schema.file"));
        Schema schema = null;

        try {
            schema = new Schema.Parser().parse(schemaFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Create a set of test records
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", "Buster");

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put("name", "Lucille");

        GenericRecord record3 = new GenericData.Record(schema);
        record3.put("name", "George Michael");

        List<GenericRecord> records = new ArrayList<>();
        records.add(record1);
        records.add(record2);
        records.add(record3);

        sendBatchToKafka(records);
    }

    /**
     * Sends a collection of GenericRecord objects to a Kafka topic. Retrieves Kafka producer
     * configuration settings from the config file to create the KafkaProducer. Serializes the
     * GenericRecord objects to byte arrays.
     *
     * @param records a List of GenericRecord objects
     */
    public static void sendBatchToKafka(List<GenericRecord> records) {

        Configuration config = ConfigurationUtility.getConfiguration(CONFIG_PATH);
        String kafkaTopic = config.getString("kafka.topic");
        String bootstrapServers = config.getString("kafka.bootstrap_servers");
        String clientID = config.getString("kafka.client_id");

        KafkaProducer producer = getProducer(bootstrapServers, clientID);

        try {
            File schemaFile = new File(config.getString("schema.file"));
            Schema schema = new Schema.Parser().parse(schemaFile);

            for(GenericRecord record : records) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
                Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(record, encoder);
                encoder.flush();
                out.close();

                byte[] recordBytes = out.toByteArray();
                ProducerRecord<String, byte[]> message = new ProducerRecord<>(kafkaTopic, recordBytes);
                producer.send(message);
            }
            producer.close();
            producer.flush();
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Instantiates a KafkaProducer using configuration properties from the config file.
     *
     * @param bootstrapServers a comma-separated list of Kafka brokers
     * @param clientID         name of the producer client
     * @return                 the KafkaProducer
     */
    private static KafkaProducer getProducer(String bootstrapServers, String clientID) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        return producer;
    }
}