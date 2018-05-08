package edu.upenn.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Page {
	@PrimaryKey
	private String addr = null;
	private String contentID = null;
	private String contentType = null;
	private long lastAccessed = -1;

	
	public Page(){
		
	}
	
	public Page(String addr, String contentID, String contentType, long lastAccessed){
		//this(addr, content, new Date(lastAccessed));
		setAddr(addr);
		setContentID(contentID);
		setContentType(contentType);
		setLastAccessed(lastAccessed);
	}

	
	public String getAddr(){
		return this.addr;
	}
	
	public String getContentID(){
		return this.contentID;
	}
	
	public String getContentType(){
		return this.contentType;
	}
	
	public long getLastAccessed(){
		return this.lastAccessed;
	}
	
	public void setAddr(String addr){
		this.addr = addr;
	}
	
	public void setContentID(String contentID){
		this.contentID = contentID;
	}
	
	public void setContentType(String contentType){
		this.contentType = contentType;
	}
	
	public void setLastAccessed(long lastAccessed){
		this.lastAccessed = lastAccessed;
	}
	
	public String toString(){
		return "URL: "+ addr+ "; Content: "+ contentID.toString()+ "; Last Accessed: "+ lastAccessed;
	}
}
