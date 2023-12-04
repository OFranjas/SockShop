package App.models;

public class Sock {
    private String sockId;
    private String type; // Example values: "invisible", "low cut", "over the calf"
    private Double price;
    private String supplierId;

    // Constructors, getters, and setters

    public Sock() {
    }

    public Sock(String sockId, String type, Double price, String supplierId) {
        this.sockId = sockId;
        this.type = type;
        this.price = price;
        this.supplierId = supplierId;
    }

    public String getSockId() {
        return sockId;
    }

    public void setSockId(String sockId) {
        this.sockId = sockId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSupplierId() {
        return supplierId;
    }

    public void setSupplierId(String supplierId) {
        this.supplierId = supplierId;
    }

    @Override
    public String toString() {
        return "Sock{" +
                "sockId='" + sockId + '\'' +
                ", type='" + type + '\'' +
                ", price=" + price +
                ", supplierId='" + supplierId + '\'' +
                '}';
    }

}
