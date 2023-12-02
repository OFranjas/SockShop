package App.purchases;

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
 * A classe Purchases simula comportamentos de compra, gerando e enviando dados
 * de compras para um tópico Kafka.
 */
public class Purchases {

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private String purchasesTopicName = "purchases_topic";
    private String dbInfoTopicName = "DBInfo_topic";
    private Random random = new Random();
    private static final Logger logger = LoggerFactory.getLogger(Purchases.class);

    public static void main(String[] args) {
        logger.info("Iniciando a aplicação Purchases...");
        Purchases purchaseApp = new Purchases();
        purchaseApp.initializeProducer();
        purchaseApp.initializeConsumer();
        purchaseApp.simulatePurchaseBehavior();
        purchaseApp.closeProducer();
        purchaseApp.closeConsumer();
    }

    /**
     * Inicializa o produtor Kafka com as configurações necessárias.
     */
    private void initializeProducer() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(producerProps);
        logger.info("Produtor Kafka inicializado");
    }

    /**
     * Inicializa o consumidor Kafka para ler dados do tópico DBInfo.
     */
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

    /**
     * Simula o comportamento de compra, consumindo dados do tópico DBInfo.
     */
    private void simulatePurchaseBehavior() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject dbInfoData = new JSONObject(record.value());
                    if (dbInfoData.has("sockId")) {
                        final String purchaseData = generatePurchaseData(dbInfoData);
                        sendPurchaseData(purchaseData);
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Consumidor sendo fechado - WakeupException");
        } catch (Exception e) {
            logger.error("Erro inesperado", e);
        } finally {
            consumer.close();
            logger.info("Consumidor Kafka fechado");
        }
    }

    /**
     * Envia dados de compra para o tópico Kafka.
     * 
     * @param purchaseData A string JSON representando os dados da compra a ser
     *                     enviada.
     */
    private void sendPurchaseData(final String purchaseData) {
        JSONObject purchaseJson = new JSONObject(purchaseData);
        String sockIdKey = purchaseJson.getString("sockId"); // Extract sockId from the purchase data

        ProducerRecord<String, String> purchaseRecord = new ProducerRecord<>(purchasesTopicName, sockIdKey,
                purchaseData);
        producer.send(purchaseRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("Erro ao enviar mensagem: {}", exception.getMessage());
                } else {
                    logger.info("Mensagem enviada: {}", purchaseData);
                }
            }
        });
    }

    /**
     * Gera dados de compra com base nas informações de meia consumidas do tópico
     * DBInfo.
     * 
     * @param sockInfo JSONObject contendo dados da meia.
     * @return Uma string JSON representando uma compra.
     */
    private String generatePurchaseData(JSONObject sockInfo) {
        JSONObject purchase = new JSONObject();
        purchase.put("sockId", sockInfo.getString("sockId"));
        purchase.put("purchasePrice", sockInfo.getDouble("price"));
        purchase.put("quantity", 1 + random.nextInt(5)); // Número aleatório de pares
        purchase.put("supplierId", sockInfo.getString("supplierId"));
        return purchase.toString();
    }

    /**
     * Fecha o produtor Kafka para liberar recursos.
     */
    private void closeProducer() {
        producer.close();
        logger.info("Produtor Kafka fechado");
    }

    /**
     * Fecha o consumidor Kafka para liberar recursos.
     */
    private void closeConsumer() {
        consumer.close();
        logger.info("Consumidor Kafka fechado");
    }
}
