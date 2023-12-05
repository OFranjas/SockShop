package App.models;

public class Sale {
    private String sock_id;
    private Double pricePerPair;
    private Integer numPairs;
    private String supplierId;
    private String buyerId;
    private String type;

    // Constructors, getters, and setters

    public Sale() {
    }

    public Sale(String sock_id, Double pricePerPair, Integer numPairs, String supplierId, String buyerId) {
        this.sock_id = sock_id;
        this.pricePerPair = pricePerPair;
        this.numPairs = numPairs;
        this.supplierId = supplierId;
        this.buyerId = buyerId;
    }

    public String getSockId() {
        return sock_id;
    }

    public void setSockId(String sock_id) {
        this.sock_id = sock_id;
    }

    public Double getPricePerPair() {
        return pricePerPair;
    }

    public void setPricePerPair(Double pricePerPair) {
        this.pricePerPair = pricePerPair;
    }

    public Integer getNumPairs() {
        return numPairs;
    }

    public void setNumPairs(Integer numPairs) {
        this.numPairs = numPairs;
    }

    public String getSupplierId() {
        return supplierId;
    }

    public void setSupplierId(String supplierId) {
        this.supplierId = supplierId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(String buyerId) {
        this.buyerId = buyerId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Sale{" +
                "sock_id='" + sock_id + '\'' +
                ", pricePerPair=" + pricePerPair +
                ", numPairs=" + numPairs +
                ", supplierId='" + supplierId + '\'' +
                ", buyerId='" + buyerId + '\'' +
                '}';
    }

}
