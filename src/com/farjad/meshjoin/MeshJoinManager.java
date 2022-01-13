package com.farjad.meshjoin;

import com.farjad.db.DatabaseHandler;
import com.farjad.db.schema.DSAttributes;
import javafx.util.Pair;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class MeshJoinManager {

    private static MeshJoinManager INSTANCE;

    public static final int NUM_PARTITIONS = 10;
    public static final int STREAM_PARTITION_SIZE = 50;
    private LinkedBlockingQueue<HashMap<DSAttributes, Object>> outputQueue = new LinkedBlockingQueue<>();

    // HashMap of <Product ID Key, Hash Map of Record's Attribute Values>
    MultiValuedMap<String, HashMap> hashTable = new ArrayListValuedHashMap<>();
    private final ArrayList<Pair<String, HashMap>> masterTableBuffer = new ArrayList<>();
    private Connection con;
    private boolean setupCompleted = false;
    private int currentMasterPartitionIndex = -1;
    private int masterDataSize;
    private int masterPartitionSize;
    private int numberOfRemovedRecords = 0;

    // SQL Queries
    private static final String MASTER_DATA_SELECT = "SELECT * FROM MASTERDATA ORDER BY PRODUCT_ID";
    private final static String MASTER_DATA_COUNT = "SELECT COUNT(*) FROM MASTERDATA";

    private MeshJoinManager() {}

    public static synchronized MeshJoinManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new MeshJoinManager();
        }
        return INSTANCE;
    }

    public void setup() {
        con = DatabaseHandler.openConnection(DatabaseHandler.DataSource.MD_SOURCE_CONN);
        fetchMasterDataSize();
        System.out.println("MeshJoin setup complete");
    }

    public void executeMeshJoin(List<HashMap<DSAttributes, Object>> newStreamPartition) {
        if (con == null) {
            setup();
        }

        // Check for EOS Marker
        if (newStreamPartition instanceof StreamQueue.EOSToken) {
            // If it has been received, then process the remaining Stream Partitions in the Queue and clean it up
            processRemainingStreamPartitions();
            getOutputQueue().add(new ETWorker.EOSToken());

            // Stream has ended, so master data doesn't need to be cycled through any more
            // Database connection can be closed
            DatabaseHandler.closeConnection(DatabaseHandler.DataSource.MD_SOURCE_CONN);
            return;
        }

        // Add all records from the new stream partition to the hash table for fast indexing based on the join key
        for (HashMap<DSAttributes, Object> record : newStreamPartition) {
            hashTable.put((String) record.get(DSAttributes.PRODUCT_ID), record);
        }

        // Load the next Master Data partition in a cyclic fashion
        loadMasterDataPartition();

        // Find the matching stream tuples for all MD tuples, using the hash table created earlier for fast indexing
        // Add the newly created tuples to the output queue
        executeJoin();

        // The oldest partition to enter the queue has been joined with all the possible MD partitions at this point
        // Hence, it should be removed from the sliding window
        removeExpiredTuples(false);

    }

    public void processRemainingStreamPartitions() {
        // Load master partitions to enrich all the remaining stream partitions
        StreamQueue streamQueue = StreamQueue.getInstance();
        System.out.println("REMAINING STREAM SIZE: " + streamQueue.getSize());
        while (streamQueue.getSize() != 0) {
            loadMasterDataPartition();
            executeJoin();
            removeExpiredTuples(true);
        }
    }

    public void executeJoin() {
        System.out.println("Master table size: " + masterTableBuffer.size());
        for (Pair<String, HashMap> masterRecordPair : masterTableBuffer) {
            String productId = masterRecordPair.getKey();
            HashMap masterRecord = masterRecordPair.getValue();

            Collection<HashMap> transactionBucket = hashTable.get(productId);

            for (HashMap transactionRecord : transactionBucket) {
                HashMap<DSAttributes, Object> joinedRecord = new HashMap<DSAttributes, Object>(transactionRecord);
                joinedRecord.putAll(masterRecord);
                joinedRecord.put(DSAttributes.TOTAL_SALE, (Integer) joinedRecord.get(DSAttributes.QUANTITY) * (Double) joinedRecord.get(DSAttributes.PRICE));

                getOutputQueue().add(joinedRecord);
                ++numberOfRemovedRecords;
            }

            if (StreamQueue.getInstance().getSize() == NUM_PARTITIONS) {
                //System.out.println("Joined " + transactionBucket.size() + " records, for productId: " + productId);
            }
        }

        if (StreamQueue.getInstance().getSize() == NUM_PARTITIONS) {
            //System.out.println("====================================");
        }
        //System.out.println("==========================================\n\n\n");
    }



    private void fetchMasterDataSize() {
        try {
            Statement statement = con.createStatement();
            ResultSet countQueryResult = statement.executeQuery(MASTER_DATA_COUNT);

            // Move cursor to first row to obtain the count value
            countQueryResult.next();
            masterDataSize = countQueryResult.getInt(1);
            masterPartitionSize = masterDataSize / 10;
            System.out.println("Master data size fetched. Partition size: " + masterPartitionSize);
        } catch (SQLException e) {

        }
    }

    private void loadMasterDataPartition() {
        masterTableBuffer.clear();
        try {
            Statement statement = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet masterDataResult = statement.executeQuery(MASTER_DATA_SELECT);

            int partitionHeadIndex = (++currentMasterPartitionIndex * masterPartitionSize) % masterDataSize;
            if (partitionHeadIndex != 0) {
                masterDataResult.absolute(partitionHeadIndex);
            }

            int recordsProcessed = 0;
            while (masterDataResult.next() && recordsProcessed < masterPartitionSize) {
                loadMasterRecord(masterDataResult);
                ++recordsProcessed;
            }

            // TODO: handle leftover records (if number of master records don't divide evenly)

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Loaded " + masterPartitionSize + " master records into buffer");
    }

    private void loadMasterRecord(ResultSet masterRecordResult) {
        HashMap<DSAttributes, Object> masterRecord = new HashMap<>();

        try {
            String productId = masterRecordResult.getString(DSAttributes.PRODUCT_ID.toString());

            masterRecord.put(DSAttributes.PRODUCT_ID, productId);
            masterRecord.put(DSAttributes.PRODUCT_NAME, masterRecordResult.getString(DSAttributes.PRODUCT_NAME.toString()));
            masterRecord.put(DSAttributes.SUPPLIER_ID, masterRecordResult.getString(DSAttributes.SUPPLIER_ID.toString()));
            masterRecord.put(DSAttributes.SUPPLIER_NAME, masterRecordResult.getString(DSAttributes.SUPPLIER_NAME.toString()));
            masterRecord.put(DSAttributes.PRICE, masterRecordResult.getDouble(DSAttributes.PRICE.toString()));

            masterTableBuffer.add(new Pair<>(productId, masterRecord));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void removeExpiredTuples(boolean streamEnded) {
        StreamQueue streamQueue = StreamQueue.getInstance();
        if (streamEnded || streamQueue.getSize() == NUM_PARTITIONS) {
            List<HashMap<DSAttributes, Object>> expiredStreamPartition = streamQueue.poll();

            for (HashMap<DSAttributes, Object> expiredStreamTuple : expiredStreamPartition) {
                String expiredTupleProductId = (String) expiredStreamTuple.get(DSAttributes.PRODUCT_ID);
                hashTable.get(expiredTupleProductId).remove(expiredStreamTuple);
            }

            //System.out.println("Removed tuples: " + expiredStreamPartition);
            System.out.println(numberOfRemovedRecords + " records joined\n" + "Removed " + expiredStreamPartition.size() + " expired tuples from Hash Table");
            //System.out.println("===============================================\n=================\n====================\n\n\n");
        }
    }

    public LinkedBlockingQueue<HashMap<DSAttributes, Object>> getOutputQueue() {
        return outputQueue;
    }
}
