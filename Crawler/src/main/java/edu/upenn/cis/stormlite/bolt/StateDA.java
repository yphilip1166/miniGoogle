package edu.upenn.cis.stormlite.bolt;

import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;

public class StateDA {
private PrimaryIndex<String, State> pIdx;
	
// provides access to State in DB
	public StateDA(EntityStore store){
		pIdx = store.getPrimaryIndex(String.class, State.class);
	}
	
	public void put(State state){
		pIdx.put(state);
	}
	
	public State get(String key){
		return pIdx.get(key);
	}
	
	public boolean containsID(String id){
		return pIdx.contains(id);
	}
	
}
