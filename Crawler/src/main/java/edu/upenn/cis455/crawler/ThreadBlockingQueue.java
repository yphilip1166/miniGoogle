package edu.upenn.cis455.crawler;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/*
 *  Generic Blocking Queue for threadpool
 */
public class ThreadBlockingQueue<T> {
	
	private Queue<T> queue;
	private HashSet<T> set;
	private int capacity;
	private final ReentrantLock lock;
	
	// constructor
	public ThreadBlockingQueue(int requested) {
		this.queue = new ArrayDeque<T>(requested);
		this.capacity = requested;
		this.lock = new ReentrantLock();
		this.set = new HashSet<T>();
	}
	
	// offer/add/enqueue
	public synchronized void offer(T thread) throws InterruptedException{

		if (isFull()){
//			wait();
			return;
		}
		if (queue.isEmpty()) notifyAll();
//		System.out.println("Adding inside blocking queue:" + thread.toString());
		if (!set.contains(thread)) {
			set.add(thread);
			queue.offer(thread);
		}
	}
	
	// poll/get/dequeue
	public synchronized T poll() throws InterruptedException{
		//System.out.println("1.");
		while (queue.isEmpty()) {
			wait();
		}
		//System.out.println("2");
		if (isFull()) notifyAll();
		//System.out.println("Socket retrieving...");
		T toReturn = queue.poll();
		//System.out.println("current queue size: "+queue.size());
		set.remove(toReturn);
		return toReturn;

	}
	
	// helper function to check if queue is full
	private boolean isFull(){
		return queue.size() >= capacity;
	}
	
	public boolean isEmpty(){
		return queue.size() == 0;
	}

	public Queue<T> getQueueCopy(){
		return new ArrayDeque<T>((ArrayDeque<T>)this.queue);
	}
}
