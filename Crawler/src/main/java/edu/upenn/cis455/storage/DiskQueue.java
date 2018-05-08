package edu.upenn.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import edu.upenn.cis455.crawler.URLWithData;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by Carlton on 4/22/18.
 */

@Entity
public class DiskQueue {
    @PrimaryKey
    private String key;
    private Queue<URLWithData> queue;

    public DiskQueue(){
        key = "DiskQueue";
        queue = new LinkedList<>();
    }

    public void offer(URLWithData url){
        queue.offer(url);
    }

    public URLWithData pull(){
        return queue.isEmpty() ? null : queue.poll();
    }

    public int size(){
        return queue.size();
    }

    public boolean isEmpty(){
        return queue.isEmpty();
    }

    public void clear(){
        queue.clear();
    }
}
