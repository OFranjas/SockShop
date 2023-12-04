package App.models;

public class Supplier {
    private String supplierId;
    private String name;
    private String contactInfo;

    // Constructors, getters, and setters

    public Supplier() {
    }

    public Supplier(String supplierId, String name, String contactInfo) {
        this.supplierId = supplierId;
        this.name = name;
        this.contactInfo = contactInfo;
    }

    public String getSupplierId() {
        return supplierId;
    }

    public void setSupplierId(String id) {
        this.supplierId = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String supplierName) {
        this.name = supplierName;
    }

    public String getContactInfo() {
        return contactInfo;
    }

    public void setContactInfo(String supplierContactInfo) {
        this.contactInfo = supplierContactInfo;
    }

    @Override
    public String toString() {
        return "Supplier{" +
                "supplierId='" + supplierId + '\'' +
                ", name='" + name + '\'' +
                ", contactInfo='" + contactInfo + '\'' +
                '}';
    }
}
