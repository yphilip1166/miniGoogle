package edu.upenn.cis455.storage;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

/**
 * Created by Carlton on 4/22/18.
 */
public class DiskQueueDA {
    private PrimaryIndex<String, DiskQueue> pIdx;

    public DiskQueueDA(EntityStore store){
        pIdx = store.getPrimaryIndex(String.class, DiskQueue.class);
    }

    public void put(DiskQueue dq){
        pIdx.put(dq);
    }

    public DiskQueue get(String url){
        return pIdx.get(url);
    }

    public boolean containsQueue(String url){
        return pIdx.contains(url);
    }
}
