package App.kafka_streams.models;

public class ProfitTypePair {
    private final double profit;
    private final String type;

    public ProfitTypePair(double profit, String type) {
        this.profit = profit;
        this.type = type;
    }

    public double getProfit() {
        return profit;
    }

    public String getType() {
        return type;
    }
}
