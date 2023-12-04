package App.kafka_streams.models;

public class PurchaseData {
    private String type;
    private double amount;

    public PurchaseData(String type, double amount) {
        this.type = type;
        this.amount = amount;
    }

    public String getType() {
        return type;
    }

    public double getAmount() {
        return amount;
    }
}
