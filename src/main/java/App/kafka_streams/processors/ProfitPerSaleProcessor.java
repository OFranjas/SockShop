package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor for calculating the profit per sock pair sale.
 */
public class ProfitPerSaleProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ProfitPerSaleProcessor.class);

    /**
     * Processes the sales and expenses data to calculate the profit per sale.
     *
     * @param builder The StreamsBuilder for building Kafka Streams applications.
     */
    @Override
    public void process(StreamsBuilder builder) {

        System.out.println("ENTROU NO PROCESS");

        // Create streams from the sales_topic and convert expenses_topic to a KTable
        KStream<String, String> salesStream = builder.stream("sales_topic");

        KTable<String, String> expensesTable = builder.table("purchases_topic");


        // Join the sales stream with the expenses KTable by a common key (e.g., sale ID)
        KStream<String, String> profitPerSaleStream = salesStream.join(
                expensesTable,
                new ValueJoiner<String, String, String>() {
                    @Override
                    public String apply(String saleValue, String expenseValue) {
                        // Parse sale and expense records to JSON
                        JSONObject sale = new JSONObject(saleValue);
                        JSONObject expense = new JSONObject(expenseValue);

                        // Extract price per pair and number of pairs from the sale record
                        double pricePerPair = sale.getDouble("pricePerPair");
                        int numPairs = sale.getInt("numPairs");

                        // Extract purchase price and quantity from the expense record
                        double purchasePrice = expense.getDouble("purchasePrice");
                        int quantity = expense.getInt("quantity");

                        // Calculate profit per sale
                        double revenue = pricePerPair * numPairs;
                        double expenseAmount = purchasePrice * quantity;
                        double profit = revenue - expenseAmount;

                        System.out.println("FEZ O CALCULO");

                        // Return the calculated profit as a string
                        return String.valueOf(profit);
                    }
                }
        );


        // Log each calculated profit
        profitPerSaleStream.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                // Log the calculated profit with a custom message format
                logger.info("✅ REQ 7 -> Calculated Profit for Sale (Sale ID: {}): {}", key, value);
            }
        });

        // Optionally, send the calculated profit to the results_topic
        profitPerSaleStream.to("results_topic");

        System.out.println("SAIU DO PROCESS");
    }
}
