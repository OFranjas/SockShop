package App.kafka_streams.models;

public class Average {
    private long count;
    private double total;

    public Average(long count, double total) {
        this.count = count;
        this.total = total;
    }

    public long getCount() {
        return count;
    }

    public double getTotal() {
        return total;
    }
}
