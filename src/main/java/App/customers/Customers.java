package App.customers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Customers {

    private KafkaProducer<String, String> producer;
    private String topicName = "sales_topic";
    private Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Customers.class);

    public static void main(String[] args) {
        logger.info("Starting Customers application...");
        Customers customerApp = new Customers();
        customerApp.initializeProducer();
        customerApp.simulateCustomerBehavior();
        customerApp.closeProducer();
    }

    private void initializeProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        logger.info("Kafka producer initialized");
    }

    private void simulateCustomerBehavior() {
        for (int i = 0; i < 10; i++) {
            final String saleData = generateSaleData();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, saleData);
            producer.send(record, new Callback() {
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
    }

    private String generateSaleData() {
        JSONObject sale = new JSONObject();
        sale.put("sockReference", "sockId" + random.nextInt(1000));
        sale.put("pricePerPair", 5 + random.nextDouble() * 10);
        sale.put("numPairs", 1 + random.nextInt(5));
        sale.put("supplierId", "supplier" + random.nextInt(100));
        sale.put("buyerId", "buyer" + random.nextInt(100));
        return sale.toString();
    }

    private void closeProducer() {
        producer.close();
        logger.info("Kafka producer closed");
    }
}
