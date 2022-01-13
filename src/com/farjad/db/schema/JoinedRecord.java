package com.farjad.db.schema;

import java.util.Date;

public class JoinedRecord {
    private String customerId;
    private String customerName;
    private String transactionId;
    private String supplierId;
    private String supplierName;
    private String storeId;
    private String storeName;
    private String productId;
    private String productName;
    private Date dateId;
    private Integer dayNum;
    private Integer monthNum;
    private Integer qtrNum;
    private Integer yearNum;
    private String dayOfWeek;
    private Integer quantity;
    private Double totalSale;

    public JoinedRecord(String customerId, String customerName, String transactionId, String supplierId,
                        String supplierName, String storeId, String storeName, String productId, String productName,
                        Date dateId, Integer dayNum, Integer monthNum, Integer qtrNum, Integer yearNum, String dayOfWeek,
                        Integer quantity, Double totalSale) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.transactionId = transactionId;
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.storeId = storeId;
        this.storeName = storeName;
        this.productId = productId;
        this.productName = productName;
        this.dateId = dateId;
        this.dayNum = dayNum;
        this.monthNum = monthNum;
        this.qtrNum = qtrNum;
        this.yearNum = yearNum;
        this.dayOfWeek = dayOfWeek;
        this.quantity = quantity;
        this.totalSale = totalSale;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getSupplierId() {
        return supplierId;
    }

    public void setSupplierId(String supplierId) {
        this.supplierId = supplierId;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public void setSupplierName(String supplierName) {
        this.supplierName = supplierName;
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Date getDateId() {
        return dateId;
    }

    public void setDateId(Date dateId) {
        this.dateId = dateId;
    }

    public Integer getDayNum() {
        return dayNum;
    }

    public void setDayNum(Integer dayNum) {
        this.dayNum = dayNum;
    }

    public Integer getMonthNum() {
        return monthNum;
    }

    public void setMonthNum(Integer monthNum) {
        this.monthNum = monthNum;
    }

    public Integer getQtrNum() {
        return qtrNum;
    }

    public void setQtrNum(Integer qtrNum) {
        this.qtrNum = qtrNum;
    }

    public Integer getYearNum() {
        return yearNum;
    }

    public void setYearNum(Integer yearNum) {
        this.yearNum = yearNum;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Double getTotalSale() {
        return totalSale;
    }

    public void setTotalSale(Double totalSale) {
        this.totalSale = totalSale;
    }
}
