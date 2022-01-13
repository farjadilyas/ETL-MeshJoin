package com.farjad.meshjoin;

import com.farjad.db.DatabaseHandler;
import com.farjad.db.schema.DSAttributes;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class ETWorker extends Thread {

    private Connection con;
    private static final String TRANSACTION_SELECT = "SELECT * FROM TRANSACTIONS";

    public static class EOSToken extends HashMap<DSAttributes, Object> {}

    @Override
    public void run() {
        try {
            con = DatabaseHandler.openConnection(DatabaseHandler.DataSource.STREAM_SOURCE_CONN);
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(TRANSACTION_SELECT);

            int limit = 10000;
            int currentCount = 0;

            while (resultSet.next()) {
                HashMap<DSAttributes, Object> streamRecord = new HashMap<>();

                streamRecord.put(DSAttributes.TRANSACTION_ID, resultSet.getInt(DSAttributes.TRANSACTION_ID.toString()));
                streamRecord.put(DSAttributes.PRODUCT_ID, resultSet.getString(DSAttributes.PRODUCT_ID.toString()));
                streamRecord.put(DSAttributes.CUSTOMER_ID, resultSet.getString(DSAttributes.CUSTOMER_ID.toString()));
                streamRecord.put(DSAttributes.CUSTOMER_NAME, resultSet.getString(DSAttributes.CUSTOMER_NAME.toString()));
                streamRecord.put(DSAttributes.STORE_ID, resultSet.getString(DSAttributes.STORE_ID.toString()));
                streamRecord.put(DSAttributes.STORE_NAME, resultSet.getString(DSAttributes.STORE_NAME.toString()));
                streamRecord.put(DSAttributes.T_DATE, resultSet.getDate(DSAttributes.T_DATE.toString()));
                streamRecord.put(DSAttributes.QUANTITY, resultSet.getInt(DSAttributes.QUANTITY.toString()));

                //System.out.println(streamRecord);

                StreamQueue.getInstance().addRecord(streamRecord);
                ++currentCount;
                if (currentCount == limit) {
                    break;
                }
            }

            // Add an EOS marker to the queue to indicate the end of the input stream
            StreamQueue.getInstance().addRecord(new EOSToken());

            resultSet.close();
            statement.close();
            DatabaseHandler.closeConnection(DatabaseHandler.DataSource.STREAM_SOURCE_CONN);
            con = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Data Producer Job Complete!");
    }
}
