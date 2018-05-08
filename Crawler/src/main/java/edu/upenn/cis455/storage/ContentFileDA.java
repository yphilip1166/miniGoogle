package edu.upenn.cis455.storage;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

/**
 * Created by Carlton on 4/23/18.
 */
public class ContentFileDA {

    private PrimaryIndex<String, ContentFile> pIdx;

    public ContentFileDA(EntityStore store){
        pIdx = store.getPrimaryIndex(String.class, ContentFile.class);
    }

    public void put(ContentFile contentFile){
        pIdx.put(contentFile);
    }

    public ContentFile get(String fileID){
        return pIdx.get(fileID);
    }

    public boolean containsFile(String fileID){
        return pIdx.contains(fileID);
    }
}
