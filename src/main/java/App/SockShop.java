package App;

import java.util.Properties;
import App.Kafka_Utils.KafkaTopicCreator;

import App.config.Config;

public class SockShop {

    public static void main(String[] args) {

        try {

            // Configure Kafka properties
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "localhost:9092"); // Kafka Broker

            // Delete Kafka topics if they already exist
            KafkaTopicCreator topicCreator = new KafkaTopicCreator(kafkaProps);
            topicCreator.deleteTopic(Config.SALES_TOPIC_DELETE);
            topicCreator.deleteTopic(Config.PURCHASES_TOPIC_DELETE);
            topicCreator.deleteTopic(Config.RESULTS_TOPIC_DELETE);
            topicCreator.deleteTopic(Config.DB_INFO_TOPIC_DELETE);

            Thread.sleep(1000);

            // Initialize Kafka topics
            createKafkaTopics(topicCreator);

            // Shutdown the Kafka topics creator when done
            topicCreator.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void createKafkaTopics(KafkaTopicCreator topicCreator) {
        // Create necessary Kafka topics
        createTopic(topicCreator, Config.SALES_TOPIC, 1, (short) 1);
        createTopic(topicCreator, Config.PURCHASES_TOPIC, 1, (short) 1);
        createTopic(topicCreator, Config.RESULTS_TOPIC, 1, (short) 1);
        createTopic(topicCreator, Config.DB_INFO_TOPIC, 1, (short) 1);
        ;
    }

    private static void createTopic(KafkaTopicCreator topicCreator, String topicName, int numPartitions,
            short replicationFactor) {
        topicCreator.createTopic(topicName, numPartitions, replicationFactor);
    }
}
