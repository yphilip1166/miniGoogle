package edu.upenn.cis455.crawler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientResponse {
	public String status = null;
	public String host = null;
	public String contentType = null;
	public String content = null;
	
	public Map<String, List<String>> header = new HashMap<>(); 
	
	// wrapper class for http/https response
	public ClientResponse(){
		
	}
	
	public void setContentType(String ct){
		this.contentType = ct;
	}
	
	public void setHeader(Map<String, List<String>> map){
		this.header = map;
	}
	
	public void setStatus(String s){
		this.status = s;
	}
	
	public void setContent(String c){
		this.content = c;
	}
	
	public void setHost(String host){
		this.host = host;
	}
	
	public void addHeader(String name, String value){
		if (header.containsKey(name)) {
			header.get(name).add(value);
		} else {
			ArrayList<String> toAdd = new ArrayList<String>();
			toAdd.add(value);
			header.put(name, toAdd);
		}
	}
}
