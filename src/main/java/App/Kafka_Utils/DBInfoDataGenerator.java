package App.Kafka_Utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class simulates a data generator that sends mock data to the DBInfo
 * Kafka topic.
 * It produces data representing information about socks and suppliers,
 * mimicking a database.
 */
public class DBInfoDataGenerator {

    private KafkaProducer<String, String> producer;
    private String topicName = "DBInfo_topic";
    private Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(DBInfoDataGenerator.class);

    public static void main(String[] args) {
        DBInfoDataGenerator dataGenerator = new DBInfoDataGenerator();
        dataGenerator.initializeProducer();
        dataGenerator.generateAndSendData();
        dataGenerator.closeProducer();
    }

    /**
     * Initializes the Kafka producer with necessary configurations for connecting
     * to Kafka.
     */
    private void initializeProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        logger.info("Kafka producer for DBInfo topic initialized");
    }

    /**
     * Continuously generates and sends mock data to the DBInfo Kafka topic.
     * Sends new data every 5 seconds.
     */
    private void generateAndSendData() {
        try {
            while (true) {
                // Generate and send sock data
                for (int i = 0; i < 10; i++) {
                    JSONObject sockData = generateSockData();
                    sendMockData(sockData.toString());
                }

                // Generate and send supplier data
                for (int i = 0; i < 5; i++) {
                    JSONObject supplierData = generateSupplierData();
                    sendMockData(supplierData.toString());
                }

                // Wait for 5 seconds before sending the next batch of data
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            logger.error("Data generation interrupted", e);
        }
    }

    /**
     * Generates mock sock data in JSON format.
     * 
     * @return A JSONObject representing a sock.
     */
    private JSONObject generateSockData() {
        JSONObject sock = new JSONObject();
        sock.put("sockId", "sock" + random.nextInt(1000));
        sock.put("type", getRandomSockType());
        sock.put("price", 5 + random.nextDouble() * 15); // Price between 5 and 20
        sock.put("supplierId", "supplier" + random.nextInt(100));
        return sock;
    }

    /**
     * Generates mock supplier data in JSON format.
     * 
     * @return A JSONObject representing a supplier.
     */
    private JSONObject generateSupplierData() {
        JSONObject supplier = new JSONObject();
        supplier.put("supplierId", "supplier" + random.nextInt(100));
        supplier.put("name", "Supplier " + random.nextInt(100));
        supplier.put("contactInfo", "Contact " + random.nextInt(100));
        return supplier;
    }

    /**
     * Selects a random sock type from a predefined list.
     * 
     * @return A string representing the sock type.
     */
    private String getRandomSockType() {
        String[] types = { "invisible", "low cut", "over the calf" };
        return types[random.nextInt(types.length)];
    }

    /**
     * Sends the generated mock data to the Kafka topic.
     * 
     * @param jsonData The JSON string representing the mock data to be sent.
     */
    private void sendMockData(final String jsonData) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, jsonData);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("Error sending mock data: {}", exception.getMessage());
                } else {
                    logger.info("Sent mock data: {}", jsonData);
                }
            }
        });
    }

    /**
     * Closes the Kafka producer to release resources.
     */
    private void closeProducer() {
        producer.close();
        logger.info("Kafka producer for DBInfo topic closed");
    }
}
