package App.purchases;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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

public class Purchases {

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private final String purchasesTopicName = "purchases_topic";
    private final String dbInfoTopicName = "DBInfo_topic";
    private final Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Purchases.class);

    public static void main(String[] args) {
        logger.info("Iniciando a aplicação Purchases...");
        Purchases purchaseApp = new Purchases();
        purchaseApp.initializeProducer();
        purchaseApp.initializeConsumer();

        // Add a shutdown hook to handle graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown request, closing Kafka producer and consumer");
            purchaseApp.consumer.wakeup();
        }));

        purchaseApp.simulatePurchaseBehavior();
    }

    private void initializeProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
        logger.info("Produtor Kafka inicializado");
    }

    private void initializeConsumer() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "purchase-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(dbInfoTopicName));
        logger.info("Consumidor Kafka inicializado e inscrito no tópico {}", dbInfoTopicName);
    }

    private void simulatePurchaseBehavior() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    JSONObject dbInfoData = new JSONObject(record.value());
                    if (dbInfoData.has("sockId")) {
                        final String purchaseData = generatePurchaseData(dbInfoData);
                        sendPurchaseData(purchaseData);
                    }
                });
            }
        } catch (WakeupException e) {
            logger.info("Consumidor sendo fechado - WakeupException");
        } catch (Exception e) {
            logger.error("Erro inesperado", e);
        } finally {
            closeConsumer();
            closeProducer();
            logger.info("Consumidor Kafka fechado");
        }
    }

    private void sendPurchaseData(final String purchaseData) {
        JSONObject purchaseJson = new JSONObject(purchaseData);
        String sockIdKey = purchaseJson.getString("sockId");

        ProducerRecord<String, String> purchaseRecord = new ProducerRecord<>(purchasesTopicName, sockIdKey,
                purchaseData);
        producer.send(purchaseRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                logger.error("Erro ao enviar mensagem: {}", exception.getMessage());
            } else {
                logger.info("Mensagem enviada: {}", purchaseData);
            }
        });
    }

    private String generatePurchaseData(JSONObject sockInfo) {
        JSONObject purchase = new JSONObject();
        purchase.put("sockId", sockInfo.getString("sockId"));
        purchase.put("purchasePrice", sockInfo.getDouble("price"));
        purchase.put("quantity", 1 + random.nextInt(5));
        purchase.put("supplierId", sockInfo.getString("supplierId"));
        purchase.put("type", sockInfo.getString("type"));
        return purchase.toString();
    }

    private void closeProducer() {
        producer.close();
        logger.info("Produtor Kafka fechado");
    }

    private void closeConsumer() {
        consumer.close();
        logger.info("Consumidor Kafka fechado");
    }
}
