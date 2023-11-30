package App;

import java.util.Properties;

import App.Kafka_Utils.KafkaTopicCreator;

public class SockShop {

    public static void main(String[] args) {
        // Configure Kafka properties
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092"); // Kafka Broker

        // Initialize Kafka topics
        KafkaTopicCreator topicCreator = new KafkaTopicCreator(kafkaProps);
        createKafkaTopics(topicCreator);

        // Start your Customers, Purchases, and Kafka Streams applications


        // Shutdown the Kafka topics creator when done
        // topicCreator.close();
    }

    private static void createKafkaTopics(KafkaTopicCreator topicCreator) {
        // Create necessary Kafka topics
        createTopic(topicCreator, "sales_topic", 1, (short) 1);
        createTopic(topicCreator, "purchases_topic", 1, (short) 1);
        createTopic(topicCreator, "results_topic", 1, (short) 1);
    }

    private static void createTopic(KafkaTopicCreator topicCreator, String topicName, int numPartitions, short replicationFactor) {
        topicCreator.createTopic(topicName, numPartitions, replicationFactor);
    }
}
