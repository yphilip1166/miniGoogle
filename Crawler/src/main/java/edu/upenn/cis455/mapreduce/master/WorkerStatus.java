package edu.upenn.cis455.mapreduce.master;

// a class to store all required information about a worker's status
public class WorkerStatus {
	private String ipAddr;
	private String status;
	private String jobName;
	private String port;
	private String keysRead;
	private String keysWritten;
	private String[] results;
	private long updateTime;
	
	public String getIpAddr(){
		return this.ipAddr;
	}
	
	public String getStatus(){
		return this.status;
	}
	
	public String getJobName(){
		return this.jobName;
	}
	
	public String getKeysRead(){
		return this.keysRead;
	}
	
	public String getKeysWritten(){
		return this.keysWritten;
	}
	
//	public WorkerStatus(String port, String status, String jobName, 
//			 String keysRead, String keysWrittern, String[] results, String ipAddr){
//		init(port, status, jobName, keysRead, keysWrittern, results);
//	}
	
	public WorkerStatus(String port, String status, String jobName,
			 String keysRead, String keysWritten, String[] results, String ipAddr){
		//this.ipAddr = ipAddr;
		this.jobName = jobName;
		this.status = status;
		this.port = port;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
		this.results = results;
		this.updateTime = System.currentTimeMillis();
		if (!ipAddr.startsWith("http"))
			this.ipAddr = "http://" + ipAddr;
		else
			this.ipAddr = ipAddr;
	}

	public WorkerStatus(String port, String status, String ipAddr, String keysRead){
		this.jobName = jobName;
		this.status = status;
		this.keysRead = keysRead;
		if (!ipAddr.startsWith("http"))
			this.ipAddr = "http://" + ipAddr;
		else
			this.ipAddr = ipAddr;
		this.updateTime = System.currentTimeMillis();
	}
	
	public boolean isValid(long interval){
		return updateTime + interval >= System.currentTimeMillis();
	}
}
