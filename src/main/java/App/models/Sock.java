package App.models;

public class Sock {
    private String sock_id;
    private String type; // Example values: "invisible", "low cut", "over the calf"
    private Double price;
    private String supplierId;

    // Constructors, getters, and setters

    public Sock() {
    }

    public Sock(String sock_id, String type, Double price, String supplierId) {
        this.sock_id = sock_id;
        this.type = type;
        this.price = price;
        this.supplierId = supplierId;
    }

    public String getSockId() {
        return sock_id;
    }

    public void setSockId(String sock_id) {
        this.sock_id = sock_id;
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
                "sock_id='" + sock_id + '\'' +
                ", type='" + type + '\'' +
                ", price=" + price +
                ", supplierId='" + supplierId + '\'' +
                '}';
    }

}
