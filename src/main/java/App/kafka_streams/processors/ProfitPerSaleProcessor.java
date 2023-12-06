package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The ProfitPerSaleProcessor class implements the KafkaStreamProcessor
 * interface. It processes a stream of sales data and a stream of purchases
 * data. It calculates the profit for each sale by subtracting the expenses from
 * the revenue. The calculated profit is then sent to another Kafka topic.
 */
public class ProfitPerSaleProcessor implements KafkaStreamProcessor {

    // Logger for logging information and calculated profit
    private static final Logger logger = LoggerFactory.getLogger(ProfitPerSaleProcessor.class);

    // ValueJoiner for calculating profit
    private final ValueJoiner<String, String, String> profitJoiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(String saleValue, String expenseValue) {
            return calculateProfit(saleValue, expenseValue);
        }
    };

    /**
     * The process method takes in a stream of sales data and a stream of purchases
     * data. It calculates the profit for each sale by subtracting the expenses from
     * the
     * revenue. The calculated profit is logged and sent to the "results_topic"
     * Kafka topic.
     *
     * @param salesStream     A stream of sales data.
     * @param purchasesStream A stream of purchases data.
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Convert the purchases streams to tables
        KTable<String, String> expensesTable = purchasesStream.toTable();

        // Join the sales stream with the expenses table, calculating the profit for
        // each sale
        KStream<String, String> profitPerSaleStream = salesStream.join(
                expensesTable,
                profitJoiner,
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        // Log the calculated profit for each sale and send it to the "results_topic"
        profitPerSaleStream
                .peek((key, value) -> logger.info("✅ REQ 7 -> Calculated Profit for Sale (Sale ID: {}): {}", key,
                        value))
                .to(Config.RESULTS_TOPIC);
    }

    /**
     * The calculateProfit method takes in a sale value and an expense value.
     * It calculates the profit by subtracting the expenses from the revenue.
     *
     * @param saleValue    The value of the sale.
     * @param expenseValue The value of the expense.
     * @return The calculated profit.
     */
    private String calculateProfit(String saleValue, String expenseValue) {
        if (expenseValue == null) {
            logger.warn("Missing Expense data for Sale: {}", saleValue);
            return "0";
        }

        JSONObject sale = new JSONObject(saleValue);
        JSONObject expense = new JSONObject(expenseValue);

        double revenue = sale.getDouble("pricePerPair") * sale.getInt("numPairs");
        double expenseAmount = expense.getDouble("purchasePrice") * expense.getInt("quantity");
        return String.valueOf(revenue - expenseAmount);
    }
}