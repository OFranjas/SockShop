package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The TotalExpensesProcessor class implements the KafkaStreamProcessor
 * interface. It processes a stream of purchases data and calculates the total
 * expenses from all purchases. The calculated total expenses are then sent to
 * another
 * Kafka topic.
 */
public class TotalExpensesProcessor implements KafkaStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TotalExpensesProcessor.class);

    /**
     * The process method takes in a stream of purchases data. It calculates the
     * total
     * expenses from all purchases by summing up the expense from each purchase. The
     * calculated
     * total expenses are logged and sent to the "results_topic" Kafka topic.
     *
     * @param salesStream     A stream of sales data (not used in this processor).
     * @param purchasesStream A stream of purchases data.
     */
    @Override
    public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {

        // Convert purchase data to expense values
        KStream<String, Double> expensesStream = purchasesStream.mapValues(value -> {
            JSONObject purchase = new JSONObject(value);
            return purchase.getDouble("purchasePrice") * purchase.getInt("quantity");
        });

        // Group the expense stream by a null key to aggregate across all records
        expensesStream.groupBy(
                (key, value) -> "TotalExpenses",
                Grouped.with(Serdes.String(), Serdes.Double()))
                // Aggregate the expenses to calculate the total expenses
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream();

        // Log the calculated total expenses
        expensesStream.foreach((key, totalExpenses) -> logger.info("✅ REQ 9 -> Total Expenses: {}", totalExpenses));

        KStream<String, String> formattedTotalExpensesStream = expensesStream.mapValues(totalExpenses -> {
            // Format total expenses to have only two decimal places
            String formattedTotalExpenses = String.format("%.2f", totalExpenses);

            JSONObject json = new JSONObject();
            json.put("requirement_id", 9); // This is for requirement 9
            json.put("result", formattedTotalExpenses);
            return json.toString();
        });

        // Send the formatted total expenses stream to the "results_topic" Kafka topic
        formattedTotalExpensesStream.to(Config.RESULTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}
