package edu.upenn.cis455.crawler;

import edu.upenn.cis455.crawler.info.URLInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClientWrapper {
	private final String UA = "cis455crawler";
	private HashMap<String, List<String>> reqHeader = new HashMap<>();
	private String url = null;
	private String method = null;
	private boolean isHTTPS = false;
	public ClientResponse cr =null;

	
	// wrapper constructor for ms2
	public ClientWrapper(String curUrl, String method){
		this.url = curUrl;
		this.method = method;
		ArrayList<String> toAdd = new ArrayList<String>();
		toAdd.add(UA);
		reqHeader.put("User-Agent", toAdd);
		
		URLInfo c = new URLInfo(url);
		ArrayList<String> toAdd2 = new ArrayList<String>();
		toAdd2.add(c.getHostName());
		reqHeader.put("Host", toAdd2);
		isHTTPS = url.toUpperCase().startsWith("HTTPS://");
	}
	
	// add header to http request
	public void addReqHeader(String key, String value){
		if (reqHeader.containsKey(key)) {
			reqHeader.get(key).add(value);
		} else {
			ArrayList<String> toAdd = new ArrayList<String>();
			toAdd.add(value);
			reqHeader.put(key, toAdd);
		}
	}
	
	// process http/https request
	public ClientResponse process() throws IOException{
		XPathCrawlerInfo.monitor(url);
		if (isHTTPS){
			HttpsClient hsc = new HttpsClient();
			hsc.setUp(url, method, reqHeader);
			return hsc.process();
		} else {
			HttpClient hpc = new HttpClient();
			hpc.setUp(url, method, reqHeader);
			return hpc.process();
		}
	}
	
	
}
