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

            // Initialize Kafka topics
            createKafkaTopics(topicCreator);

            // Shutdown the Kafka topics creator when done
            topicCreator.close();

            // Starting the DBInfo Data Generator application
            Thread dbInfoDataGeneratorThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    DBInfoDataGenerator.main(new String[0]);
                }
            });
            dbInfoDataGeneratorThread.start();

            // Wait for the DBInfo Data Generator to start
            Thread.sleep(2000);

            // Starting the Customers application
            Thread customersThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Customers.main(new String[0]);
                }
            });
            customersThread.start();

            // Wait for the Customers application to start
            Thread.sleep(2000);

            // Starting the Purchases application
            Thread purchasesThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Purchases.main(new String[0]);
                }
            });
            purchasesThread.start();

            // Wait for the Purchases application to start
            Thread.sleep(2000);

            // Starting the Kafka Streams application
            Thread kafkaStreamsThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    KafkaStreamsApp.main(new String[0]);
                }
            });
            kafkaStreamsThread.start();

            Thread.sleep(2000);

            // Wait for threads completion
            kafkaStreamsThread.join();
            dbInfoDataGeneratorThread.join();
            customersThread.join();
            purchasesThread.join();
        } catch (InterruptedException e) {

            Properties kafkaProps = new Properties();
            kafkaProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            // Delete Kafka topics
            KafkaTopicCreator topicCreator = new KafkaTopicCreator(kafkaProps);
            topicCreator.deleteTopic("sales_topic");
            topicCreator.deleteTopic("purchases_topic");
            topicCreator.deleteTopic("results_topic");
            topicCreator.deleteTopic("DBInfo_topic");
            topicCreator.close();

            System.out.println("Fim da execução");

        } catch (Exception e) {
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
