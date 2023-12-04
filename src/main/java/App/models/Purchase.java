package App.models;

public class Purchase {
    private String sockId;
    private Double purchasePrice;
    private Integer quantity;
    private String supplierId;
    private String type;

    // Constructors, getters, and setters

    public Purchase() {
    }

    public Purchase(String sockId, Double purchasePrice, Integer quantity, String supplierId) {
        this.sockId = sockId;
        this.purchasePrice = purchasePrice;
        this.quantity = quantity;
        this.supplierId = supplierId;
    }

    public String getSockId() {
        return sockId;
    }

    public void setSockId(String sockId) {
        this.sockId = sockId;
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
                "sockId='" + sockId + '\'' +
                ", purchasePrice=" + purchasePrice +
                ", quantity=" + quantity +
                ", supplierId='" + supplierId + '\'' +
                '}';
    }

}
