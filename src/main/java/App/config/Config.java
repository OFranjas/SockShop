package App.config;

public class Config {

    // Kafka related configuration
    public static final String KAFKA_BROKER = "localhost:9092,localhost:9093,localhost:9094";

    // Topic names
    public static final String SALES_TOPIC = "sales_test";
    public static final String PURCHASES_TOPIC = "purchases_test";
    public static final String RESULTS_TOPIC = "result_test";
    public static final String DB_INFO_TOPIC = "DBInfo_test";

    // Topics to delete 
    public static final String SALES_TOPIC_DELETE = "sales4_topic";
    public static final String PURCHASES_TOPIC_DELETE = "purchases4_topic";
    public static final String RESULTS_TOPIC_DELETE = "result_topic";
    public static final String DB_INFO_TOPIC_DELETE = "DBInfo4_topic";

    // Stream configurations
    public static final String APPLICATION_ID = "Kafka18";

    // Other global constants
    // Add other global constants that might be needed in your application

    // Prevent instantiation
    private Config() {
    }
}
