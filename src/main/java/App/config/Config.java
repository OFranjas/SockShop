package App.config;

public class Config {

    // Kafka related configuration
    public static final String KAFKA_BROKER = "localhost:9092,localhost:9093,localhost:9094";

    // Topic names
    public static final String SALES_TOPIC = "sales";
    public static final String PURCHASES_TOPIC = "purchases";
    public static final String RESULTS_TOPIC = "results";
    public static final String DB_INFO_TOPIC = "DBInfo";

    // Topics to delete 
    public static final String SALES_TOPIC_DELETE = "sales";
    public static final String PURCHASES_TOPIC_DELETE = "purchases";
    public static final String RESULTS_TOPIC_DELETE = "results";
    public static final String DB_INFO_TOPIC_DELETE = "DBInfo";

    // Stream configurations
    public static final String APPLICATION_ID = "K50";

    // Other global constants
    // Add other global constants that might be needed in your application

    // Prevent instantiation
    private Config() {
    }
}
