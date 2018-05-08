package edu.upenn.cis.stormlite.bolt;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import java.util.ArrayList;
import java.util.HashMap;

@Entity
public class State {
	@PrimaryKey
	private String id = null;
	private HashMap<String, ArrayList<String>> map = null;
	
	// a class to be used with reduce bolt
	public State(){
		map = new HashMap<>();
	}
	
	public State(String id){
		setID(id);
		map = new HashMap<>();
	}
	
	public HashMap<String, ArrayList<String>> getMap(){
		return this.map;
	}
	
	public void put(String key, String value){
		if (!map.containsKey(key)){
			map.put(key, new ArrayList<String>());
		} 
		map.get(key).add(value);
	}
	
	public String getID(){
		return this.id;
	}
	
	public void setID(String id){
		this.id = id;
	}
}
