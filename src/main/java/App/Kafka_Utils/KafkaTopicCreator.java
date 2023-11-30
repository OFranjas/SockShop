package App.Kafka_Utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Properties;
import java.util.Arrays;

public class KafkaTopicCreator {

    private final AdminClient adminClient;

    public KafkaTopicCreator(Properties kafkaProps) {
        // Create an AdminClient using the provided Kafka properties
        adminClient = AdminClient.create(kafkaProps);
    }

    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        // Create a new Kafka topic if it doesn't exist
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        try {
            adminClient.createTopics(Arrays.asList(newTopic)).all().get();
        } catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) {
                // Topic already exists, no action needed
                System.out.println("Topic '" + topicName + "' already exists.");
            } else {
                e.printStackTrace();
                // Handle other errors during topic creation
            }
        }
    }

    public void close() {
        // Close the AdminClient when done
        adminClient.close();
    }
}
