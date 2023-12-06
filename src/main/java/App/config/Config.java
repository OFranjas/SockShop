package App.config;

public class Config {

    // Kafka related configuration
    public static final String KAFKA_BROKER = "localhost:9092,localhost:9093,localhost:9094";

    // Topic names
    public static final String SALES_TOPIC = "sales1_test";
    public static final String PURCHASES_TOPIC = "purchases1_test";
    public static final String RESULTS_TOPIC = "result1_test";
    public static final String DB_INFO_TOPIC = "DBInfo1_test";

    // Topics to delete 
    public static final String SALES_TOPIC_DELETE = "sales_topic";
    public static final String PURCHASES_TOPIC_DELETE = "purchases_topic";
    public static final String RESULTS_TOPIC_DELETE = "result_topic";
    public static final String DB_INFO_TOPIC_DELETE = "DBInfo_topic";

    // Stream configurations
    public static final String APPLICATION_ID = "K32";

    // Other global constants
    // Add other global constants that might be needed in your application

    // Prevent instantiation
    private Config() {
    }
}
