package App.Kafka_Utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.customers.Customers;

import java.util.Properties;
import java.util.Arrays;

public class KafkaTopicCreator {

    private final AdminClient adminClient;

    private static final Logger logger = LoggerFactory.getLogger(Customers.class);

    public KafkaTopicCreator(Properties kafkaProps) {
        // Create an AdminClient using the provided Kafka properties
        adminClient = AdminClient.create(kafkaProps);
    }

    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        // Create a new Kafka topic if it doesn't exist
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        try {
            adminClient.createTopics(Arrays.asList(newTopic)).all().get();

            logger.info("Topic '" + topicName + "' created.");
        } catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) {
                // Topic already exists, no action needed
                logger.warn("Topic '" + topicName + "' already exists.");
            } else {
                e.printStackTrace();
                // Handle other errors during topic creation
            }
        }
    }

    // Delete a Kafka topic
    public void deleteTopic(String topicName) {
        try {
            adminClient.deleteTopics(Arrays.asList(topicName)).all().get();

            logger.info("Topic '" + topicName + "' deleted.");
        } catch (Exception e) {
            e.printStackTrace();
            // Handle other errors during topic deletion
        }
    }


    public void close() {
        // Close the AdminClient when done
        adminClient.close();
    }
}
