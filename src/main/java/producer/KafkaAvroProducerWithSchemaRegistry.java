package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import utility.ConfigurationUtility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaAvroProducerWithSchemaRegistry {

    private static final String CONFIG_PATH = "src/main/resources/config.properties";

    public static void main(String[] args) {
        try {
            Configuration conf = ConfigurationUtility.getConfiguration("src/main/resources/config.properties");
            File schemaFile = new File(conf.getString("schema.file"));
            Schema schema = new Schema.Parser().parse(schemaFile);

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

            KafkaAvroProducerWithSchemaRegistry.sendBatchToKafka(records);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Retrieves Kafka producer configuration settings from the config file to create the KafkaProducer.
     * Sends a collection of GenericRecord objects to a Kafka topic.
     *
     * @param records a List of GenericRecord objects
     */
    public static void sendBatchToKafka(List<GenericRecord> records) {
        Configuration config = ConfigurationUtility.getConfiguration(CONFIG_PATH);
        String kafkaTopic = config.getString("kafka.topic");
        String bootstrapServers = config.getString("kafka.bootstrap_servers");
        String clientID = config.getString("kafka.client_id");
        String schemaRegistryURL = config.getString("kafka.schema.registry.url");

        KafkaProducer producer = getProducer(bootstrapServers, clientID, schemaRegistryURL);

        for (GenericRecord record : records) {
            ProducerRecord<Long, GenericRecord> message = new ProducerRecord<>(kafkaTopic, record);
            producer.send(message);
        }

        producer.flush();
        producer.close();
    }

    /**
     * Instantiates a KafkaProducer using configuration properties from the config file.
     *
     * @param bootstrapServers  a comma-separated list of Kafka brokers
     * @param clientID          name of the producer client
     * @param schemaRegistryURL URL of schema registry
     * @return the KafkaProducer
     */
    private static KafkaProducer getProducer(String bootstrapServers, String clientID, String schemaRegistryURL) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        KafkaProducer producer = new KafkaProducer(kafkaProperties);
        return producer;
    }
}