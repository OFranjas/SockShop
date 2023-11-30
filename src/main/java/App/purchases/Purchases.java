package App.purchases;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import java.util.Properties;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Purchases {

    private KafkaProducer<String, String> producer;
    private String topicName = "purchases_topic";
    private Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Purchases.class);

    public static void main(String[] args) {
        Purchases purchasesApp = new Purchases();
        purchasesApp.initializeProducer();
        purchasesApp.simulatePurchases();
        purchasesApp.closeProducer();
    }

    private void initializeProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        logger.info("Kafka producer for purchases initialized");
    }

    private void simulatePurchases() {
        for (int i = 0; i < 10; i++) {
            final String purchaseData = generatePurchaseData();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, purchaseData);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        logger.error("Error sending purchase data: {}", exception.getMessage());
                    } else {
                        logger.info("Sent purchase data: {}", purchaseData);
                    }
                }
            });
        }
    }

    private String generatePurchaseData() {
        JSONObject purchase = new JSONObject();
        purchase.put("sockId", "sock" + random.nextInt(1000));
        purchase.put("price", 10 + random.nextDouble() * 20); // Price between 10 and 30
        purchase.put("numPairs", 1 + random.nextInt(5)); // Number of pairs between 1 and 5
        purchase.put("type", getRandomSockType());
        purchase.put("supplierId", "supplier" + random.nextInt(100));
        return purchase.toString();
    }

    private String getRandomSockType() {
        String[] types = {"invisible", "low cut", "over the calf"};
        return types[random.nextInt(types.length)];
    }

    private void closeProducer() {
        producer.close();
        logger.info("Kafka producer for purchases closed");
    }
}
