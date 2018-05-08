package edu.upenn.cis455.mapreduce.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.URLQueueUpdator;
import edu.upenn.cis455.crawler.URLWithData;
import edu.upenn.cis455.crawler.XPathCrawlerInfo;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static spark.Spark.setPort;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {
        
    static DistributedCluster cluster = new DistributedCluster();
    
    List<TopologyContext> contexts = new ArrayList<>();

    int myPort;
        
    static List<String> topologies = new ArrayList<>();
    
    static String masterDir = null;
    
    static String storageDir = null;
    
    static int portNum = -1;

    static int workerIdx = -1;
        
    public WorkerServer(int myPort) throws MalformedURLException {
                
//        log.info("Creating server listener at socket " + myPort);
       
        setPort(myPort);
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        
        Spark.post("/definejob", new Route() {
	        @SuppressWarnings("static-access")
			@Override
	        public Object handle(Request arg0, Response arg1) {
	            WorkerJob workerJob;
	            try {
	                workerJob = om.readValue(arg0.body(), WorkerJob.class);

                    String startUrl = null;
                    String dbPath = null;
                    int maxSize = 0;
                    int numFile = -1;

	                try {
	                	// send job information to worker
	                    System.out.println(workerJob.getConfig().get("job"));
	                	contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(), 
	                                                        workerJob.getTopology()));
	                	Config con = workerJob.getConfig();

//                        if (con.containsKey("seedurl")) {
//                            startUrl = con.get("seedurl");
//                        }

//                        if (con.containsKey("DBPath")) {
//                            dbPath = con.get("DBPath");
//                        }

                        if (con.containsKey("maxSize")) {
                            maxSize = Integer.parseInt(con.get("maxSize"));
                        }

                        if (con.containsKey("numFile")) {
                            numFile = Integer.parseInt(con.get("numFile"));
                        }


                        XPathCrawlerInfo xci = new XPathCrawlerInfo();
                        xci.setArgs(startUrl, storageDir, maxSize, numFile);
                        xci.loadUrlFromDisk();

//                        // add seed url by worker idx;
                        if (workerIdx == 0) {
                              xci.putURL(new URLWithData("https://www.yahoo.com/news/weather"));
                              xci.putURL(new URLWithData("https://www.yahoo.com"));

//                            xci.putURL(new URLWithData("http://www.ign.com/"));
//                            xci.putURL(new URLWithData("https://fujilove.com/"));
//                            xci.putURL(new URLWithData("https://www.sony.com/"));
//                            xci.putURL(new URLWithData("https://www.ferrari.com/en-US"));
//                            xci.putURL(new URLWithData("https://www.shopbop.com/"));
//                            xci.putURL(new URLWithData("https://www.farfetch.com/"));
//                            xci.putURL(new URLWitData("https://www.ssense.com"));
//                            xci.putURL(new URLWithData("http://www.revolve.com/"));
//                            xci.putURL(new URLWithData("https://www.vogue.com/"));
//                            xci.putURL(new URLWithData("http://www.nvidia.com/page/home.html"));
//                            xci.putURL(new URLWithData("https://www.amd.com/en"));
//                            xci.putURL(new URLWithData("https://www.massdrop.com/"));
//                            xci.putURL(new URLWithData("https://gizmodo.com/"));
//                            xci.putURL(new URLWithData("http://www.tennis.com/"));
//                            xci.putURL(new URLWithData("http://www.nba.com/"));
//                            xci.putURL(new URLWithData("https://www.usnews.com"));
//                            xci.putURL(new URLWithData("https://docs.oracle.com"));
//                            xci.putURL(new URLWithData("https://docs.oracle.com/cd/E17277_02/html/GettingStartedGuide/BerkeleyDB-JE-GSG.pdf"));
//                            xci.putURL(new URLWithData("http://www.ucsd.edu"));
//                            xci.putURL(new URLWithData("https://en.wikipedia.org/wiki/Main_Page"));
//                            xci.putURL(new URLWithData("https://www.vox.com/"));
//                            xci.putURL(new URLWithData("https://www.npr.org/"));
//                            xci.putURL(new URLWithData("https://www.youtube.com/"));
//                            xci.putURL(new URLWithData("http://www.bbc.com/"));
//                            xci.putURL(new URLWithData("https://www.ebay.com/"));
//                            xci.putURL(new URLWithData("https://www.upenn.edu/"));
//                            xci.putURL(new URLWithData("https://stackoverflow.com/"));
//                            xci.putURL(new URLWithData("http://www.espn.com"));
//                            xci.putURL(new URLWithData("https://www.yahoo.com/news/weather/sitemap.xml"));
//                            xci.putURL(new URLWithData("https://www.reddit.com/"));
//                            xci.putURL(new URLWithData("http://www.fujifilmusa.com/index.html"));
//                            xci.putURL(new URLWithData("https://www.usa.canon.com/internet/portal/us/home"));
//                            xci.putURL(new URLWithData("https://www.nikonusa.com/en/index.page"));
//                            xci.putURL(new URLWithData("https://www.starbucks.com/"));
//                            xci.putURL(new URLWithData("https://www.msn.com/en-us"));
//                            xci.putURL(new URLWithData("https://www.google.com/"));
//                            xci.putURL(new URLWithData("https://www.adobe.com/"));
//                            xci.putURL(new URLWithData("https://www.pinterest.com/"));
//                            xci.putURL(new URLWithData("https://twitter.com/?lang=en"));
//                            xci.putURL(new URLWithData("https://www.craigslist.org/about/sites"));
//                            xci.putURL(new URLWithData("https://www.gamespot.com/"));
//                            xci.putURL(new URLWithData("https://www.healthaffairs.org/"));
//                            xci.putURL(new URLWithData("https://www.yahoo.com/"));
//                            xci.putURL(new URLWithData("https://www.sephora.com"));
//                            xci.putURL(new URLWithData("https://www.linkedin.com/"));
//                            xci.putURL(new URLWithData("http://www.septa.org/"));
//                            xci.putURL(new URLWithData("http://www.fifa.com/"));
//                            xci.putURL(new URLWithData("https://www.moma.org"));
                        }else if(workerIdx == 1){

//                            xci.putURL(new URLWithData("https://www.facebook.com"));
//                            xci.putURL(new URLWithData("https://www.bhphotovideo.com"));
//                            xci.putURL(new URLWithData("http://www.cis.upenn.edu/~ahae"));
//                            xci.putURL(new URLWithData("http://www.cis.upenn.edu"));
//                            xci.putURL(new URLWithData("http://acsweb.ucsd.edu/~peyan/"));
//                            xci.putURL(new URLWithData("https://www.imdb.com/"));

//                            xci.putURL(new URLWithData("http://www.philamuseum.org/"));
//                            xci.putURL(new URLWithData("https://www.rottentomatoes.com/"));
//                            xci.putURL(new URLWithData("https://www.buzzfeed.com/"));
//                            xci.putURL(new URLWithData("https://www.usatoday.com/"));
//                            xci.putURL(new URLWithData("https://www.bmwusa.com/"));
//                            xci.putURL(new URLWithData("https://www.mbusa.com/"));
//                            xci.putURL(new URLWithData("https://www.audiusa.com/"));
//                            xci.putURL(new URLWithData("https://www.tripadvisor.com/"));
//                            xci.putURL(new URLWithData("https://www.expedia.com/"));
//                            xci.putURL(new URLWithData("https://www.airbnb.com"));
//                            xci.putURL(new URLWithData("https://www.forbes.com"));
//                            xci.putURL(new URLWithData("https://www.hotels.com/"));
//                            xci.putURL(new URLWithData("https://www.theverge.com/"));
//                            xci.putURL(new URLWithData("https://weather.com/"));
//                            xci.putURL(new URLWithData("https://www.amazon.com/"));
//                            xci.putURL(new URLWithData("https://www.bloomberg.com/"));
//                            xci.putURL(new URLWithData("https://www.wired.com/"));
//                            xci.putURL(new URLWithData("https://www.apple.com/"));
//                            xci.putURL(new URLWithData("https://www.nytimes.com/"));
//                            xci.putURL(new URLWithData("https://www.marketwatch.com"));

                        }else if(workerIdx == 2){




                        }else if(workerIdx == 3){


                        }

                        URLQueueUpdator urlQueueUpdator = new URLQueueUpdator();
                        urlQueueUpdator.start();
	                    synchronized (topologies) {
	                        topologies.add(workerJob.getConfig().get("job"));
	                    }
	                } catch (ClassNotFoundException e) {
	                    e.printStackTrace();
	                } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "Job launched";
	            } catch (IOException e) {
	                e.printStackTrace();
	                                
	                // Internal server error
	                arg1.status(500);
	                return e.getMessage();
	            } 
	                
	        }
        
        });
        
	    Spark.post("/runjob", new Route() {
            @Override
            public Object handle(Request arg0, Response arg1) {
                cluster.startTopology();
                return "Started";
            }
	     });
        
	    Spark.post("/push/:stream", new Route() {
            @Override
            public Object handle(Request arg0, Response arg1) {
                try {
                    String stream = arg0.params(":stream");
                    Tuple tuple = om.readValue(arg0.body(), Tuple.class);

                                    
                    // Find the destination stream and route to it
                    StreamRouter router = cluster.getStreamRouter(stream);
                                    
                    if (contexts.isEmpty())

                                    
                    if (!tuple.isEndOfStream())
                        contexts.get(contexts.size() - 1).incSendOutputs(router.getKey(tuple.getValues()));
                                    
                    if (tuple.isEndOfStream())
                        router.executeEndOfStreamLocally(contexts.get(contexts.size() - 1));
                    else
                        router.executeLocally(tuple, contexts.get(contexts.size() - 1));
                                    
                    return "OK";
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                                    
                    arg1.status(500);
                    return e.getMessage();
                }
                            
            }
	            
	    });
	    
	    Spark.post("/shutdown", new Route() {
	    	@Override
	    	public Object handle(Request arg0, Response arg1){
	    		System.out.println("Shutdown command received");
	    		WorkerServer.shutdown();
	    		return "Shutdown";
	    	}
	     });

    }
        
    public static void createWorker(Map<String, String> config) {
        if (!config.containsKey("masterDir"))
            throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");

        if (!config.containsKey("storeDir"))
            throw new RuntimeException("Store directory not provided");
        
        if (!config.containsKey("port"))
        	throw new RuntimeException("No port number provided");

        if (!config.containsKey("workerIdx")){
            throw new RuntimeException("No worker index provided");
        }

        try {
        	Integer.parseInt(config.get("port"));
            Integer.parseInt(config.get("workerIdx"));
        } catch (NumberFormatException e){
        	throw new RuntimeException("Port number error:" + config.get("port"));
        }
        
        int port = Integer.parseInt(config.get("port"));
        int idx = Integer.parseInt(config.get("workerIdx"));

        System.out.println("Initializing worker " + port);

        try {
        	masterDir = config.get("masterDir");
        	storageDir = config.get("storeDir");
        	portNum = port;
            workerIdx = idx;
            new WorkerServer(port);
            WorkerServerInfo.setPort(portNum);
            WorkerServerInfo.setStore(storageDir);
            WorkerServerInfo.setMaster(masterDir);
            WorkerServerInfo.setAWSClient(config.get("awsKey"), config.get("awsID"));
            WorkerServerInfo.clearResults();
            WorkerServerInfo wsi = new WorkerServerInfo();
            System.out.println("Starting pinging");
            
            wsi.start();

            PageRankUpdator pru = new PageRankUpdator();
            pru.start();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    public static void shutdown() {
        synchronized(topologies) {
            for (String topo: topologies)
                cluster.killTopology(topo);
        }
        System.out.println("Cluster shutting down");
        cluster.shutdown();
        XPathCrawlerInfo.storeUrlToDisk();
        System.exit(0);
    }
}
