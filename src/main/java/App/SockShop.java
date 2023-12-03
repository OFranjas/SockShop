package App;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;

import App.Kafka_Utils.DBInfoDataGenerator;
import App.Kafka_Utils.KafkaTopicCreator;
import App.customers.Customers;
import App.kafka_streams.KafkaStreamsApp;
import App.purchases.Purchases;

public class SockShop {

    public static void main(String[] args) {

        try {

            // Configure Kafka properties
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", "localhost:9092"); // Kafka Broker

            // Delete Kafka topics if they already exist
            KafkaTopicCreator topicCreator = new KafkaTopicCreator(kafkaProps);
            topicCreator.deleteTopic("sales_topic");
            topicCreator.deleteTopic("purchases_topic");
            topicCreator.deleteTopic("results_topic");
            topicCreator.deleteTopic("DBInfo_topic");

            // Initialize Kafka topics
            createKafkaTopics(topicCreator);

            // Shutdown the Kafka topics creator when done
            topicCreator.close();

        }  catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void createKafkaTopics(KafkaTopicCreator topicCreator) {
        // Create necessary Kafka topics
        createTopic(topicCreator, "sales_topic", 1, (short) 1);
        createTopic(topicCreator, "purchases_topic", 1, (short) 1);
        createTopic(topicCreator, "results_topic", 1, (short) 1);
        createTopic(topicCreator, "DBInfo_topic", 1, (short) 1);
        ;
    }


    private static void createTopic(KafkaTopicCreator topicCreator, String topicName, int numPartitions,
            short replicationFactor) {
        topicCreator.createTopic(topicName, numPartitions, replicationFactor);
    }
}
