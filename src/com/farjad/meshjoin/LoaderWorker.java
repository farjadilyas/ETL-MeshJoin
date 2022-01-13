package com.farjad.meshjoin;

import com.farjad.db.DatabaseHandler;
import com.farjad.db.schema.DSAttributes;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class LoaderWorker extends Thread {
    private LinkedBlockingQueue<HashMap<DSAttributes, Object>> etlOutputQueue;
    private Connection outputConn;
    private int recordCount = 0;

    private static final String INSERT_INTO_CUSTOMERS = "INSERT IGNORE INTO CUSTOMERS VALUES(?,?)";
    private static final String INSERT_INTO_SUPPLIERS = "INSERT IGNORE INTO SUPPLIERS VALUES(?,?)";
    private static final String INSERT_INTO_STORES = "INSERT IGNORE INTO STORES VALUES(?,?)";
    private static final String INSERT_INTO_DATES = "INSERT IGNORE INTO TIME_DIM VALUES(?,?,?,?,?,?)";
    private static final String INSERT_INTO_PRODUCTS = "INSERT IGNORE INTO PRODUCTS VALUES(?,?)";
    private static final String INSERT_INTO_SALES = "INSERT IGNORE INTO SALES VALUES(?,?,?,?,?,?,?,?)";
    private static final String[] DAYS_OF_WEEK = {"NaN", "SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY",
        "THURSDAY", "FRIDAY", "SATURDAY"};

    private final HashSet<String> loadedCustomerIds = new HashSet<>();
    private final HashSet<String> loadedSupplierIds = new HashSet<>();
    private final HashSet<String> loadedStoreIds = new HashSet<>();
    private final HashSet<Date> loadedDateIds = new HashSet<>();
    private final HashSet<String> loadedProductIds = new HashSet<>();

    public LoaderWorker() {
        etlOutputQueue = MeshJoinManager.getInstance().getOutputQueue();
        outputConn = DatabaseHandler.openConnection(DatabaseHandler.DataSource.DW_OUTPUT_CONN);
    }

    @Override
    public void run() {
        while (true) {
            try {
                HashMap<DSAttributes, Object> outputRecord = etlOutputQueue.take();

                // If EOS token is met, then no more output tuples will be received from the Transformation Phase
                // We can break from this loop & fill data into out Warehouse Schema
                if (outputRecord instanceof ETWorker.EOSToken) {
                    System.out.println("ETL Loader: EOS met, with " + recordCount + " records");
                    break;
                }

                loadOutputRecord(outputRecord);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        DatabaseHandler.closeConnection(DatabaseHandler.DataSource.DW_OUTPUT_CONN);
    }

    private void loadOutputRecord(HashMap<DSAttributes, Object> record) {
        ++recordCount;
        //System.out.println("Record #" + ++recordCount + ": " + record);

        // Add any missing data in dimension tables - data that the new fact table record will depend on
        if (!loadedCustomerIds.contains(record.get(DSAttributes.CUSTOMER_ID))) {
            addCustomerRecord((String) record.get(DSAttributes.CUSTOMER_ID), (String) record.get(DSAttributes.CUSTOMER_NAME));
        }

        if (!loadedSupplierIds.contains(record.get(DSAttributes.SUPPLIER_ID))) {
            addSupplierRecord((String) record.get(DSAttributes.SUPPLIER_ID), (String) record.get(DSAttributes.SUPPLIER_NAME));
        }

        if (!loadedStoreIds.contains(record.get(DSAttributes.STORE_ID))) {
            addStoreRecord((String) record.get(DSAttributes.STORE_ID), (String) record.get(DSAttributes.STORE_NAME));
        }

        if (!loadedDateIds.contains(record.get(DSAttributes.T_DATE))) {
            addDateRecord((Date) record.get(DSAttributes.T_DATE));
        }

        if (!loadedProductIds.contains(record.get(DSAttributes.PRODUCT_ID))) {
            addProductRecord((String) record.get(DSAttributes.PRODUCT_ID), (String) record.get(DSAttributes.PRODUCT_NAME));
        }

        // Add the new fact table record corresponding to this joined output record
        addSaleRecord((Integer) record.get(DSAttributes.TRANSACTION_ID),
                (String) record.get(DSAttributes.PRODUCT_ID),
                (String) record.get(DSAttributes.CUSTOMER_ID),
                (String) record.get(DSAttributes.STORE_ID),
                (Date) record.get(DSAttributes.T_DATE),
                (String) record.get(DSAttributes.SUPPLIER_ID),
                (Integer) record.get(DSAttributes.QUANTITY),
                (Double) record.get(DSAttributes.TOTAL_SALE));
    }

    private void addCustomerRecord(String customerId, String customerName) {
        try {
            PreparedStatement addCustomerStatement = outputConn.prepareStatement(INSERT_INTO_CUSTOMERS);
            addCustomerStatement.setString(1, customerId);
            addCustomerStatement.setString(2, customerName);
            addCustomerStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        loadedCustomerIds.add(customerId);
    }

    private void addSupplierRecord(String supplierId, String supplierName) {
        try {
            PreparedStatement addSupplierStatement = outputConn.prepareStatement(INSERT_INTO_SUPPLIERS);
            addSupplierStatement.setString(1, supplierId);
            addSupplierStatement.setString(2, supplierName);
            addSupplierStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        loadedSupplierIds.add(supplierId);
    }

    private void addStoreRecord(String storeId, String storeName) {
        try {
            PreparedStatement addStoreStatement = outputConn.prepareStatement(INSERT_INTO_STORES);
            addStoreStatement.setString(1, storeId);
            addStoreStatement.setString(2, storeName);
            addStoreStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        loadedStoreIds.add(storeId);
    }

    private void addDateRecord(Date dateId) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(dateId);

        try {
            PreparedStatement addDateStatement = outputConn.prepareStatement(INSERT_INTO_DATES);
            addDateStatement.setDate(1, (java.sql.Date) dateId);
            addDateStatement.setInt(2, calendar.get(Calendar.DAY_OF_MONTH));
            addDateStatement.setInt(3, calendar.get(Calendar.MONTH)+1);
            addDateStatement.setInt(4, (calendar.get(Calendar.MONTH) + 3)/3);
            addDateStatement.setInt(5, calendar.get(Calendar.YEAR));
            addDateStatement.setString(6, DAYS_OF_WEEK[calendar.get(Calendar.DAY_OF_WEEK)]);
            addDateStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        loadedDateIds.add(dateId);
    }

    private void addProductRecord(String productId, String productName) {
        try {
            PreparedStatement addProductStatement = outputConn.prepareStatement(INSERT_INTO_PRODUCTS);
            addProductStatement.setString(1, productId);
            addProductStatement.setString(2, productName);
            addProductStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        loadedProductIds.add(productId);
    }

    private void addSaleRecord(Integer transactionId, String productId, String customerId, String storeId, Date dateId, String supplierId, Integer quantity, Double totalSale) {
        try {
            PreparedStatement addSaleStatement = outputConn.prepareStatement(INSERT_INTO_SALES);
            addSaleStatement.setInt(1, transactionId);
            addSaleStatement.setString(2, productId);
            addSaleStatement.setString(3, customerId);
            addSaleStatement.setString(4, storeId);
            addSaleStatement.setDate(5, (java.sql.Date) dateId);
            addSaleStatement.setString(6, supplierId);
            addSaleStatement.setInt(7, quantity);
            addSaleStatement.setDouble(8, totalSale);
            addSaleStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
