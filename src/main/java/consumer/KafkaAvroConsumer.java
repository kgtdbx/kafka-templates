package consumer;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utility.ConfigurationUtility;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumer {

    private static final String CONFIG_PATH = "src/main/resources/config.properties";

    public static void main(String[] args) {
        getMessages();
    }

    private static void getMessages() {
        KafkaConsumer consumer = getConsumer();

        int maximumPoll = 200;
        int pollCount = 0;

        while (true) {
            final ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                System.out.println("NO RECORDS");
                pollCount++;
                if (pollCount > maximumPoll) {
                    break;
                }
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Key: %d, Value: %s, Partition: %d, Offset: %d\n",
                        record.key(), record.value(), record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
    }

    private static KafkaConsumer getConsumer() {
        Configuration config = ConfigurationUtility.getConfiguration(CONFIG_PATH);
        String kafkaTopic = config.getString("kafka.topic");
        String bootstrapServers = config.getString("kafka.bootstrap_servers");
        String groupID = config.getString("kafka.group_id");

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaProperties);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

        return kafkaConsumer;
    }
}