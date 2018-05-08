package edu.upenn.cis455.mapreduce.worker;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayDeque;

public class WorkerServerInfo extends Thread{
	private static String serverDir = "";
	private static int port = 0;
	private static String jobName = "";
	private static String status = "INIT";
	private static String storeDir = "";
	private static String inputDir = "";
	private static String outputDir = "";
	private static int keysRead = 0;
	private static int keysWritten = 0;
	private static ArrayDeque<String> results = new ArrayDeque<String>();
	private static String awsKey = "";
	private static String awsID = "";
	private static AmazonS3 s3client = null;
	
	private static boolean running = true;
	private static final int maxResults = 100;
	private static final int duration = 10000;
	
	// new thread that send worker status every 10 seconds
		@Override 
		public void run(){
			while (running) {
				String urlStr = "http://" + serverDir + "/workerstatus?" + "port=" + port + "&status=" + status + "&keysRead=" + keysRead;
				URL url;
				System.out.println(urlStr);
				try {
					url = new URL(urlStr);
					HttpURLConnection conn = (HttpURLConnection)url.openConnection();
					conn.setDoOutput(true);
					conn.setRequestMethod("GET");
					conn.getResponseCode();
					conn.getResponseMessage();

				} catch (MalformedURLException e) {
					//e.printStackTrace();
				} catch (ProtocolException e) {
					//e.printStackTrace();
				} catch (IOException e) {
					//e.printStackTrace();
				} finally {
					try {
						Thread.sleep(duration);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	
	public static String getInput(){
		return inputDir;
	}
	
	public static String getOutput(){
		return outputDir;
	}
	
	public static void setInput(String input){
		inputDir = input;
	}
	
	public static void setOutput(String output){
		outputDir = output;
	}
	
	public static String getStore(){
		return storeDir;
	}
	
	public static void reset(){
		keysRead = 0;
		keysWritten = 0;
		clearResults();
	}
	
	public static void resetCounter(){
		keysRead = 0;
		keysWritten = 0;
	}
	
	public static synchronized void incrementKeysRead(){
		++keysRead;
	}

	public static void incrementKeysWritten(int i){
		keysWritten+=i;
	}

	public static void incrementKeysWritten(){
		++keysWritten;
	}
	
	public static void setPort(int p){
		port = p;
	}
	
	public static void setMaster(String dir){
		serverDir = dir;
	}
	
	public static void setJobName(String job){
		jobName = job;
	}
	
	public static void setStatus(String s){
		status = s;
	}
	
	public static void clearResults(){
		results.clear();
	}
	
	public static void setStore(String d){
		storeDir = d;
	}
	
	public static void addResult(String result){
		if (results.size() == maxResults) results.pollFirst();
		results.add(result);
	}

	public static void setAWSClient(String key, String id){
		key = key.split("\n")[0];
		id = id.split("\n")[0];
		awsKey = key;
		awsID = id;
		AWSCredentials credentials = new BasicAWSCredentials(awsKey, awsID);
		s3client = new AmazonS3Client(credentials);
	}

	public static AmazonS3 getAWSClient(){
			return s3client;
	}

}
