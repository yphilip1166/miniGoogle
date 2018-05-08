package edu.upenn.cis455.crawler;

import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.DiskQueue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class XPathCrawlerInfo {

	private String startUrl = null;
	private String dbPath = null;
	private static int numFile = -1;
	private String hostname = null;
	private static ThreadBlockingQueue<URLWithData> queue = null;
	private final static String allCrawler = "*";
	private final static String crawlerName = "cis455crawler";
	private static ConcurrentHashMap<String, RobotsTxtInfo> robotCache = null;
	private static ConcurrentHashMap<String, Long> accessMap = null;
	private static Monitor monitor = null;
	private static int numCrawled = 0;
	public static long maxSize = 0;
	public static LocalCluster cluster = null;
	public static String clusterName = null;
	public static RobotsTxtInfo defaultrti = new RobotsTxtInfo();
	
	public XPathCrawlerInfo(){
		this.queue = new ThreadBlockingQueue<URLWithData>(100000);
		this.robotCache = new ConcurrentHashMap<String, RobotsTxtInfo>();
		this.accessMap = new ConcurrentHashMap<String, Long>();
		String hostname = "cis455.cis.upenn.edu";
		try {
			this.monitor = new Monitor(hostname);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void storeUrlToDisk() {
        DiskQueue dq = new DiskQueue();
        Queue<URLWithData> toStoreQueue = queue.getQueueCopy();
        while (!toStoreQueue.isEmpty()) {
            dq.offer(toStoreQueue.poll());
        }
        DBWrapper.putQueue(dq);
	}

	public static void loadUrlFromDisk(){
        try {
            if (DBWrapper.containsQueue("DiskQueue")){
                DiskQueue dq = DBWrapper.getQueue("DiskQueue");
                while (!dq.isEmpty()){
                    queue.offer(dq.pull());
                }
            }
        } catch (InterruptedException e){
            System.out.println(e);
        } catch (ClassCastException e){
			System.out.println(e);
		}
    }
	
	public static void checkTerminate(){
		if (numFile > 0 && numCrawled >= numFile) {
			terminateCluster();
		}
	}
	
	public static void terminateCluster(){
		if (cluster != null && clusterName!=null){
			try{
				cluster.killTopology(clusterName);
				cluster.shutdown();
				System.exit(0);
			} catch (Exception e){
				System.out.println(e);
			}
		}
	}
	
	public void setCluster(LocalCluster c, String cn){
		cluster = c;
		clusterName = cn;
	}
	
	// init database
	private void startDB(){
		DBWrapper.initDBStore(dbPath);
	}
	
	public void setArgs(String startUrl, String dbpath, int maxSize, int numFile){
		this.startUrl = startUrl;
		this.dbPath = dbpath;
		this.maxSize = maxSize*1024*1024;
		this.numFile = numFile;
		
		try {
			if (startUrl != null) {
				System.out.println("Adding seed url" + startUrl);
				queue.offer(new URLWithData(startUrl));
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		startDB();
	}
	
	public static void monitor(String url) throws IOException{
		monitor.send(url);
	}
	
	synchronized public static void increment(){
		++numCrawled;
	}
	
	public static URLWithData getURL() throws InterruptedException{

		return queue.poll();
	}
	
	public static void putURL(URLWithData urlToPut) throws InterruptedException {
		queue.offer(urlToPut);
	}
	
	// if robot is contained
	public static boolean containsRobot(String query){
		return robotCache.containsKey(query);
	}
	
	// get robots.txt 
	private static ClientResponse retrieveRobot(String curUrl) throws IOException {
		URL c = new URL(curUrl);
		String robotUrl = c.getProtocol()+"://"+c.getHost()+"/robots.txt";
		ClientWrapper cw = new ClientWrapper(robotUrl, "GET");
		ClientResponse cr = cw.process();
		return cr;
	}
	
	// check if robots.txt is satisfied
	private static boolean checkRobot(String curUrl, RobotsTxtInfo rti) throws MalformedURLException, InterruptedException{
		URL cu = new URL(curUrl);
		String curPath = cu.getPath();
		
//		System.out.println("Checking Robot");
		//rti.print();
		
		if (rti.containsUserAgent(crawlerName)){
			if (containsUrl(rti.getDisallowedLinks(crawlerName), curPath)){
				System.out.println("Link:"+ curUrl+" is not allowed by crawler name");
				return false;
			}
		}else if (rti.containsUserAgent(allCrawler) && containsUrl(rti.getDisallowedLinks(allCrawler), curPath)){
			System.out.println("Link not allowed by *");
			return false;
		}
		
		//System.out.println("Robot pass");
		return true;
	}
	
	// helper function to check if url included
	private static boolean containsUrl(List<String> list, String url){
		if (list == null) return false;
		for (String u: list){
			if (url.startsWith(u)){
				return true;
			}
		}
		return false;
	}
	
	public static int passRobot(String curUrl) throws IOException, InterruptedException{

		URL temp = new URL(curUrl);
		String query = temp.getHost();
		
		RobotsTxtInfo rti = defaultrti;
		if (containsRobot(query)){
			//System.out.println("Getting robot from cache");
			rti = getRobot(query);
		} else {
			System.out.println("Downloading robot");
			ClientResponse robotRes = retrieveRobot(curUrl);
			if (robotRes != null) {
				rti = RobotParser.parse(robotRes.content);
				putRobot(query, rti);
			} else {
				putRobot(query, defaultrti);
			}
		}
		
		boolean passRobot = checkRobot(curUrl, rti);
		
		if (!passRobot){
			System.out.println("not pass robot.txt");
			return -1;
		}
		
		if (!containsLA(query)) {
			putLA(query, System.currentTimeMillis());
			return 1;
		}
		//System.out.println("Passed Robot, checking delay");
		int crawlDelay = 1;
		
		if (containsRobot(query)){
			rti = getRobot(query);
			if (rti.getCrawlDelay(crawlerName)!=null){
				crawlDelay = rti.getCrawlDelay(crawlerName);
//					System.out.println("delay is: " + crawlDelay);
			} else if (rti.getCrawlDelay(allCrawler)!=null){
				crawlDelay = rti.getCrawlDelay(allCrawler);
//					System.out.println("delay is: " + crawlDelay);
			}
		}
		
		crawlDelay *= 1000;
		
		long fileLA = getLA(query);
		//System.out.println(query+" LA:"+ fileLA+"; Now:"+ System.currentTimeMillis()+"; Delay:"+crawlDelay);
		if (crawlDelay > System.currentTimeMillis() - fileLA){
			//System.out.println("Too soon");
			return 0;
		}
		putLA(query, System.currentTimeMillis());
		return 1;
	}
	
	// add robot to cache
	public static void putRobot(String query, RobotsTxtInfo content){
		if(robotCache.size() > 10000){
			robotCache.clear();
		}
		robotCache.put(query, content);
	}
	
	// retrieve robot info
	public static RobotsTxtInfo getRobot(String query){
			return robotCache.get(query);
	}
	
	// if lass accessed info is present
	public static boolean containsLA(String query){
			return accessMap.containsKey(query);
	}
	
	// add last accessed info
	public static void putLA(String query, long la){
		accessMap.put(query, la);
	}
	
	// retrieve last accessed info
	public static long getLA(String query){
			return accessMap.get(query);
	}
}
