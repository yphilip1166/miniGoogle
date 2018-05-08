package edu.upenn.cis455.storage;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;


public class PageDA {
	private PrimaryIndex<String, Page> pIdx;
	
	public PageDA(EntityStore store){
		pIdx = store.getPrimaryIndex(String.class, Page.class);
	}
	
	public void put(Page cf){
		pIdx.put(cf);
	}
	
	public Page get(String addr){
		return pIdx.get(addr);
	}
	
	public boolean containsFile(String addr){
		return pIdx.contains(addr);
	}
}
