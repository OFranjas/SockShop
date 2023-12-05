package App.customers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.models.Sale;
import App.config.Config;

public class Customers {

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private final String salesTopicName = Config.SALES_TOPIC;
    private final String dbInfoTopicName = Config.DB_INFO_TOPIC;
    private final Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Customers.class);

    public static void main(String[] args) {
        logger.info("Starting Customers application...");
        Customers customerApp = new Customers();
        customerApp.initializeProducer();
        customerApp.initializeConsumer();

        // Add a shutdown hook to handle graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown request, closing Kafka producer and consumer");
            customerApp.consumer.wakeup();
        }));

        customerApp.simulateCustomerBehavior();
    }

    private void initializeProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
        logger.info("Kafka producer initialized");
    }

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

    private void simulateCustomerBehavior() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject recordJson = new JSONObject(record.value());
                    if (recordJson.has("payload")) {
                        JSONObject payload = recordJson.getJSONObject("payload");
                        sendSaleData(generateSaleData(payload));
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Consumer closing - WakeupException");
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        } finally {
            closeConsumer();
            closeProducer();
            logger.info("Kafka consumer closed");
        }
    }

    private void sendSaleData(final String saleData) {
        /// print the sale data
        // System.out.println("\n\n\nSale data: " + saleData + "\n\n\n");

        JSONObject saleJson = new JSONObject(saleData);

        // print the sale json

        // System.out.println("\n\n\nSale json: " + saleJson.toString() + "\n\n\n");

        // Extracting sock_id from saleJson
        String sockIdKey = saleJson.getString("sockId");

        ProducerRecord<String, String> saleRecord = new ProducerRecord<>(salesTopicName, sockIdKey, saleData);
        producer.send(saleRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                logger.error("Error sending message: {}", exception.getMessage());
            } else {
                logger.info("Sent message: {}", saleData);
            }
        });

    }

    private String generateSaleData(JSONObject payload) {

        // PRINT PAYLOAD

        System.out.println("\n\n\nPayload: " + payload.toString() + "\n\n\n");

        // Create a new Sale object
        Sale sale = new Sale();
        sale.setSockId(payload.optString("sock_id", "")); // Use optString for potential null values
        sale.setPricePerPair(payload.optDouble("price", 0.0)); // Use optDouble for potential null values
        sale.setNumPairs(1 + random.nextInt(5)); // Use optInt with default value
        sale.setSupplierId(payload.optString("supplierid", ""));
        sale.setBuyerId("buyer" + random.nextInt(100));
        sale.setType(payload.optString("type", ""));

        // Serialize the Sale object to a JSON string
        return new JSONObject(sale).toString();
    }

    private void closeProducer() {
        producer.close();
        logger.info("Kafka producer closed");
    }

    private void closeConsumer() {
        consumer.close();
        logger.info("Kafka consumer closed");
    }
}
