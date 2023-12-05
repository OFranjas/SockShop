package App.models;

public class Purchase {
    private String sock_id;
    private Double purchasePrice;
    private Integer quantity;
    private String supplierId;
    private String type;

    // Constructors, getters, and setters

    public Purchase() {
    }

    public Purchase(String sock_id, Double purchasePrice, Integer quantity, String supplierId) {
        this.sock_id = sock_id;
        this.purchasePrice = purchasePrice;
        this.quantity = quantity;
        this.supplierId = supplierId;
    }

    public String getSockId() {
        return sock_id;
    }

    public void setSockId(String sock_id) {
        this.sock_id = sock_id;
    }

    public Double getPurchasePrice() {
        return purchasePrice;
    }

    public void setPurchasePrice(Double pricePerPair) {
        this.purchasePrice = pricePerPair;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer numPairs) {
        this.quantity = numPairs;
    }

    public String getSupplierId() {
        return supplierId;
    }

    public void setSupplierId(String supplierId) {
        this.supplierId = supplierId;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "Purchase{" +
                "sock_id='" + sock_id + '\'' +
                ", purchasePrice=" + purchasePrice +
                ", quantity=" + quantity +
                ", supplierId='" + supplierId + '\'' +
                '}';
    }

}
