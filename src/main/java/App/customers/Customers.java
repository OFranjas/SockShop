package App.customers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

/**
 * The Customers class simulates customer behaviors by generating and sending
 * sales data to a Kafka topic.
 */
public class Customers {

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private String salesTopicName = "sales_topic";
    private String dbInfoTopicName = "DBInfo_topic";
    private Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Customers.class);

    public static void main(String[] args) {
        logger.info("Starting Customers application...");
        Customers customerApp = new Customers();
        customerApp.initializeProducer();
        customerApp.initializeConsumer();
        customerApp.simulateCustomerBehavior();
        customerApp.closeProducer();
        customerApp.closeConsumer();
    }

    /**
     * Initializes the Kafka producer with necessary configurations.
     */
    private void initializeProducer() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(producerProps);
        logger.info("Kafka producer initialized");
    }

    /**
     * Initializes the Kafka consumer to read data from the DBInfo topic.
     */
    private void initializeConsumer() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(dbInfoTopicName));
        logger.info("Kafka consumer initialized and subscribed to {}", dbInfoTopicName);
    }

    /**
     * Continuously simulates customer behavior by consuming data from the DBInfo
     * topic.
     */
    private void simulateCustomerBehavior() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject dbInfoData = new JSONObject(record.value());
                    if (dbInfoData.has("sockId")) { // Simple check to filter sock data
                        final String saleData = generateSaleData(dbInfoData);
                        sendSaleData(saleData);
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Consumer closing - WakeupException");
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            consumer.close();
            logger.info("Kafka consumer closed");
        }
    }

    /**
     * Sends sale data to the Kafka topic.
     * 
     * @param saleData The JSON string representing the sale data to be sent.
     */
    private void sendSaleData(final String saleData) {
        ProducerRecord<String, String> saleRecord = new ProducerRecord<>(salesTopicName, saleData);
        producer.send(saleRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("Error sending message: {}", exception.getMessage());
                } else {
                    logger.info("Sent message: {}", saleData);
                }
            }
        });
    }

    /**
     * Generates sale data based on the sock information consumed from DBInfo topic.
     * 
     * @param sockInfo JSONObject containing sock data.
     * @return A JSON string representing a sale.
     */
    private String generateSaleData(JSONObject sockInfo) {
        JSONObject sale = new JSONObject();
        sale.put("sockReference", sockInfo.getString("sockId"));
        sale.put("pricePerPair", sockInfo.getDouble("price"));
        sale.put("numPairs", 1 + random.nextInt(5)); // Random number of pairs
        sale.put("supplierId", sockInfo.getString("supplierId"));
        sale.put("buyerId", "buyer" + random.nextInt(100));
        return sale.toString();
    }

    /**
     * Closes the Kafka producer to release resources.
     */
    private void closeProducer() {
        producer.close();
        logger.info("Kafka producer closed");
    }

    /**
     * Closes the Kafka consumer to release resources.
     */
    private void closeConsumer() {
        consumer.close();
        logger.info("Kafka consumer closed");
    }
}
