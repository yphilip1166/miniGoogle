package edu.upenn.cis455.mapreduce.master;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.CrawlerBolt;
import edu.upenn.cis.stormlite.bolt.FilterBolt;
import edu.upenn.cis.stormlite.bolt.ParserBolt;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.CrawlerSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// helper class for MasterServlet
public class MasterServletInfo {
	private HashMap<String, WorkerStatus> statusMap = new HashMap<>();
	static Logger log = Logger.getLogger(MasterServletInfo.class);
	private final long activePeriod = 1000 * 30;
	private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String MAP_BOLT = "MAP_BOLT";
    private static final String REDUCE_BOLT = "REDUCE_BOLT";
    private static final String PRINT_BOLT = "PRINT_BOLT";

    private static final String C_SPOUT = "CRAWLER_SPOUT";
    private static final String C_BOLT = "CRAWLER_BOLT";
    private static final String P_BOLT = "PRASER_BOLT";
    private static final String F_BOLT = "FILTER_BOLT";


    private Config config;
	
	public MasterServletInfo(){
		
	}
	
	// add status 
	public void putStatus(String key, WorkerStatus val){
		statusMap.put(key, val);
	}
	
	// get a status
	public WorkerStatus getStatus(String key){
		return statusMap.get(key);
	}
	
	// find all active workers
	public HashMap<String, WorkerStatus> getActiveWorkers(){
		Iterator<String> iterator = statusMap.keySet().iterator();
		
		// delete inactive workers
		while (iterator.hasNext()){
			String key = iterator.next();
			if (!statusMap.get(key).isValid(activePeriod)){
				iterator.remove();
			}
		}
		
		return this.statusMap;
	}

	public void submitCrawlerJob(Config config) throws IOException {

		CrawlerSpout spout = new CrawlerSpout();
		CrawlerBolt cbolt = new CrawlerBolt();
		ParserBolt pbolt = new ParserBolt();
		FilterBolt fbolt = new FilterBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(C_SPOUT, spout, 6);

        // TODO
        builder.setBolt(C_BOLT, cbolt, 10).fieldsGrouping(C_SPOUT, new Fields("HostName"));
//        builder.setBolt(C_BOLT, cbolt, 4).shuffleGrouping(C_SPOUT);

        // TODO
        builder.setBolt(P_BOLT, pbolt, 10).fieldsGrouping(C_BOLT, new Fields("HostName"));
//        builder.setBolt(P_BOLT, pbolt, 4).shuffleGrouping(C_BOLT);

        // TODO
        builder.setBolt(F_BOLT, fbolt, 10).fieldsGrouping(P_BOLT, new Fields("HostName"));
//        builder.setBolt(F_BOLT, fbolt, 4).shuffleGrouping(P_BOLT);

        Topology topo = builder.createTopology();

        WorkerJob job = new WorkerJob(topo, config);

        ObjectMapper mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);




        config.put("workerList", createWorkerList());
        this.config = config;

        try {
            String[] workers = WorkerHelper.getWorkers(config);

            int i = 0;
            for (String dest: workers) {
                config.put("workerIndex", String.valueOf(i++));
                if (sendJob(dest, "POST", config, "definejob",
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() !=
                        HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job definition request failed");
                }
            }

            for (String dest: workers) {
                if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() !=
                        HttpURLConnection.HTTP_OK) {
                    throw new RuntimeException("Job execution request failed");
                }
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private String createWorkerList(){
        StringBuilder wl = new StringBuilder("[");
        synchronized (statusMap){
            for (Map.Entry<String, WorkerStatus> entry: getActiveWorkers().entrySet()){
                wl.append(entry.getValue().getIpAddr()).append(",");
            }
            if (wl.charAt(wl.length()-1) == ',') wl.deleteCharAt(wl.length()-1);
            wl.append("]");
        }
        return wl.toString();
    }
	
	// submit a job to workers
	public void submitJob(Config config, int numMap, int numReduce) throws IOException{

//		WorkerServer.createWorker(config);
//
//		FileSpout spout = new WordFileSpout();
//        MapBolt bolt = new MapBolt();
//        ReduceBolt bolt2 = new ReduceBolt();
//        PrintBolt printer = new PrintBolt();
//
//	    TopologyBuilder builder = new TopologyBuilder();
//
//	    builder.setSpout(WORD_SPOUT, spout, Integer.valueOf(config.get("spoutExecutors")));
//
//        // Parallel mappers, each of which gets specific words
//        builder.setBolt(MAP_BOLT, bolt, Integer.valueOf(config.get("mapExecutors"))).fieldsGrouping(WORD_SPOUT, new Fields("value"));
//
//        // Parallel reducers, each of which gets specific words
//        builder.setBolt(REDUCE_BOLT, bolt2, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));
//
//        // Only use the first printer bolt for reducing to a single point
//        builder.setBolt(PRINT_BOLT, printer, 1).firstGrouping(REDUCE_BOLT);
//
//	    Topology topo = builder.createTopology();
//
//		WorkerJob job = new WorkerJob(topo, config);
//
//		ObjectMapper mapper = new ObjectMapper();
//        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
//        StringBuilder wl = new StringBuilder("[");
//        synchronized (statusMap){
//        	for (Map.Entry<String, WorkerStatus> entry: getActiveWorkers().entrySet()){
//        		wl.append(entry.getValue().getIpAddr()).append(",");
//        		System.out.println("Adding:"+entry.getValue().getIpAddr());
//        	}
//        	if (wl.charAt(wl.length()-1) == ',') wl.deleteCharAt(wl.length()-1);
//        	wl.append("]");
//        }
//        System.out.println("Size is:" + statusMap.size());
//        config.put("workerList", wl.toString());
//        this.config = config;
//		try {
//			String[] workers = WorkerHelper.getWorkers(config);
//
//			int i = 0;
//			for (String dest: workers) {
//		        config.put("workerIndex", String.valueOf(i++));
//				if (sendJob(dest, "POST", config, "definejob",
//						mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job)).getResponseCode() !=
//						HttpURLConnection.HTTP_OK) {
//					throw new RuntimeException("Job definition request failed");
//				}
//			}
//			for (String dest: workers) {
//				if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() !=
//						HttpURLConnection.HTTP_OK) {
//					throw new RuntimeException("Job execution request failed");
//				}
//			}
//		} catch (JsonProcessingException e) {
//			e.printStackTrace();
//	        System.exit(0);
//		}
	}
	
	// send shutdown request
	public void sendShutdown() throws IOException{
		// get all workers
		StringBuilder wl = new StringBuilder("[");
		synchronized (statusMap){
        	for (Map.Entry<String, WorkerStatus> entry: getActiveWorkers().entrySet()){
        		wl.append(entry.getValue().getIpAddr()).append(",");
        		System.out.println("Adding:"+entry.getValue().getIpAddr());
        	}
        	if (wl.charAt(wl.length()-1) == ',') wl.deleteCharAt(wl.length()-1);
        	wl.append("]");
        }
		Config c = new Config();
		c.put("workerList", wl.toString());
		String[] workers = WorkerHelper.getWorkers(c);
		
		// send shutdown request to all workers
		for (String dest: workers){
			System.out.println("Sending shutdown to:" + dest);
			try {
				int i = sendJob(dest, "POST", c, "shutdown", "").getResponseCode();
				System.out.println(i);
			} catch (Exception e){
				// expected since worker is closed
			}
		}
		
	}
	
	// send http request
	static HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException {
		URL url = new URL(dest + "/" + job);
		
		log.info("Sending request to " + url.toString());
		
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);
		
		if (reqType.equals("POST")) {
			conn.setRequestProperty("Content-Type", "application/json");
			
			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		} else
			conn.getOutputStream();
		
		return conn;
    }
}
