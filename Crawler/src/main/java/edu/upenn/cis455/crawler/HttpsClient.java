package edu.upenn.cis455.crawler;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class HttpsClient {
	private String url = null;
	private String method = null;
	private String lineSep = System.getProperty("line.separator");
	//private HttpsURLConnection huc = null;
	private BufferedReader br = null;
	private HashMap<String, List<String>> reqHeader = new HashMap<>();
	private ClientResponse cr = new ClientResponse();;
	
	public HttpsClient(){
		
	}
	
	// set up request info
	public void setUp(String url, String method, HashMap<String, List<String>> req){
		setUrl(url);
		setMethod(method);
		setReqHeader(req);
	}
	
	public void setReqHeader(HashMap<String, List<String>> req){
		this.reqHeader = req;
	}
	
	public void setMethod(String method){
		this.method = method;
	}
	
	public void setUrl(String url){
		this.url = url;
	}
	
	// send https request
	public ClientResponse process(){
		HttpsURLConnection huc = null;
		try{
			URL curl = new URL(url);
			huc =  (HttpsURLConnection) curl.openConnection();
			huc.setConnectTimeout(10000);
			send(huc);
			this.br = new BufferedReader(new InputStreamReader(huc.getInputStream()));
			recv(huc);
		} catch (IOException e){
			System.out.println(e);
			System.out.println("returning null client response");
			return null;
		} finally {
			if (huc != null){
				huc.disconnect();
			}
			if (this.br!=null) {
				try{
					br.close();
				} catch (IOException e){

				}

			}
		}
		return this.cr;
	}
	
	// receive https response
	private void recv(HttpsURLConnection huc) throws IOException{
		parseStatus(huc);
		parseHeader(huc);
		parseContent(huc);
	}
	
	private void parseContent(HttpsURLConnection huc) throws IOException{
		String line;
		StringBuilder sb = new StringBuilder();
		while ((line = br.readLine())!=null){
			sb.append(line).append(lineSep);
		}
		cr.setContent(sb.toString());
		br.close();
	}
	
	private void parseHeader(HttpsURLConnection huc){
		for (Entry<String, List<String>> entry: huc.getHeaderFields().entrySet()){
			for (String str: entry.getValue()){
				if (entry.getKey() != null){
					cr.addHeader(entry.getKey().toLowerCase(), str);
				}
			}
		}
	}
	
	private void parseStatus(HttpsURLConnection huc) throws IOException{
		cr.setStatus(huc.getResponseCode()+"");
	}
	
	private void send(HttpsURLConnection huc) throws IOException{
		huc.setInstanceFollowRedirects(false);
		setMethod(huc);
		setHeader(huc);
		huc.connect();
	}

	private void setHeader(HttpsURLConnection huc){
		for (Entry<String, List<String>> entry: reqHeader.entrySet()){
			for (String str: entry.getValue()){
				huc.setRequestProperty(entry.getKey(), str);
			}
		}
	}
	
	private void setMethod(HttpsURLConnection huc) throws ProtocolException{
//		System.out.println("Setting method: "+ method);
		huc.setRequestMethod(method);
	}

}
