package App.kafka_streams.processors;

import App.kafka_streams.KafkaStreamProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import App.config.Config;

/**
 * The TotalProfitProcessor class implements the KafkaStreamProcessor
 * interface. It processes a stream of sales data and a stream of purchases
 * data. It calculates the total profit by subtracting the total expenses from
 * the total revenue. The calculated total profit is then sent to another Kafka
 * topic.
 */
public class TotalProfitProcessor implements KafkaStreamProcessor {

        private static final Logger logger = LoggerFactory.getLogger(TotalProfitProcessor.class);

        /**
         * The process method takes in a stream of sales data and a stream of purchases
         * data. It calculates the total profit by subtracting the total expenses from
         * the
         * total revenue. The calculated total profit is logged and sent to the
         * "results_topic" Kafka topic.
         *
         * @param salesStream     A stream of sales data.
         * @param purchasesStream A stream of purchases data.
         */
        @Override
        public void process(KStream<String, String> salesStream, KStream<String, String> purchasesStream) {
                // Convert sales stream to a KTable
                KTable<String, String> salesTable = salesStream.toTable();

                // Convert purchases stream to a KTable
                KTable<String, String> expensesTable = purchasesStream.toTable();

                // Join the sales and expenses tables to calculate profit for each sale
                KTable<String, Double> profitPerSaleTable = salesTable.join(
                                expensesTable,
                                (saleValue, expenseValue) -> calculateProfit(saleValue, expenseValue));

                // Ensure each unique sale is only processed once
                KTable<String, Double> distinctProfitPerSaleTable = profitPerSaleTable.groupBy(
                                (key, value) -> KeyValue.pair(key, value),
                                Grouped.with(Serdes.String(), Serdes.Double()))
                                .reduce(
                                                (aggValue, newValue) -> newValue, // Adder - always take the new value
                                                (aggValue, oldValue) -> aggValue // Subtractor - retain the existing
                                                                                 // value
                                );

                // Group the profit table by a constant key to aggregate across all records
                KGroupedStream<String, Double> groupedProfit = distinctProfitPerSaleTable.toStream().groupBy(
                                (key, value) -> "total",
                                Grouped.with(Serdes.String(), Serdes.Double()));

                // Aggregate the profits to calculate the total profit
                groupedProfit
                                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                                .toStream()
                                .peek((key, totalProfit) -> logger.info("✅ REQ 10 -> Total Profit: {}", totalProfit))
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
        private Double calculateProfit(String saleValue, String expenseValue) {
                if (expenseValue == null) {
                        logger.warn("Missing Expense data for Sale: {}", saleValue);
                        return 0.0;
                }

                JSONObject sale = new JSONObject(saleValue);
                JSONObject expense = new JSONObject(expenseValue);

                double revenue = sale.getDouble("pricePerPair") * sale.getInt("numPairs");
                double expenseAmount = expense.getDouble("purchasePrice") * expense.getInt("quantity");
                return revenue - expenseAmount;
        }
}
