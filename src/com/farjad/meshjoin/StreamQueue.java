package com.farjad.meshjoin;

import com.farjad.db.schema.DSAttributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamQueue {

    private static StreamQueue INSTANCE = null;
    private final ConcurrentLinkedQueue<List<HashMap<DSAttributes, Object>>> queue = new ConcurrentLinkedQueue<>();

    private int currentStreamPartitionIndex = 0;
    private List<HashMap<DSAttributes, Object>> streamBuffer;

    private StreamQueue() {}

    public class EOSToken extends ArrayList<HashMap<DSAttributes, Object>> {}

    public static StreamQueue getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new StreamQueue();
        }
        return INSTANCE;
    }

    /*
      Fills up the stream buffer being maintained
      Once the buffer reaches its maximum capacity, the buffer is offered to the stream queue in the form of a partition
     */
    public synchronized void addRecord(HashMap<DSAttributes, Object> record) {
        // If the stream has ended, then add an EOS marker in the shared queue
        if (record instanceof ETWorker.EOSToken) {
            if (streamBuffer != null && streamBuffer.size() > 0) {
                queue.offer(streamBuffer);
                MeshJoinManager.getInstance().executeMeshJoin(streamBuffer);
                streamBuffer = null;
            }

            System.out.println("Indicate EOS to MeshJoin");
            EOSToken eosToken = new EOSToken();
            MeshJoinManager.getInstance().executeMeshJoin(eosToken);
        }

        if (streamBuffer == null) {
            ++currentStreamPartitionIndex;
            streamBuffer = new ArrayList<>();
        }

        streamBuffer.add(record);
        if (streamBuffer.size() == MeshJoinManager.STREAM_PARTITION_SIZE) {
            System.out.println("Current partition is now full. Enqueuing it into Stream Partition Queue");
            queue.offer(streamBuffer);
            MeshJoinManager.getInstance().executeMeshJoin(streamBuffer);
            streamBuffer = null;
        }
    }

    public synchronized List<HashMap<DSAttributes, Object>> poll() {
        return queue.poll();
    }

    public int getSize() {
        return queue.size();
    }
}
