package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.URLWithData;
import edu.upenn.cis455.crawler.XPathCrawlerInfo;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.UUID;

public class FilterBolt implements IRichBolt{
	static Logger log = Logger.getLogger(FilterBolt.class);
		
	Fields myFields = new Fields();
	OutputCollector collector;
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

	@Override
	public void cleanup() {
		// Do nothing

	}

	@Override
	public void execute(Tuple input) {
//		System.out.println("Filter:" + getExecutorId() + ": " + input.toString());
//		HashSet<String> toReturn = (HashSet<String>)input.getObjectByField("linkList");
//		for (String url: toReturn){
			// simple filter on protocol
		String url = input.getStringByField("cururl");
		String la = input.getStringByField("lastAccessed");
		String fromUrl = input.getStringByField("FromURL");
		if (validUrl(url)){
			try {
				XPathCrawlerInfo.putURL(new URLWithData(url, la, fromUrl));
		//                    System.out.println("Filter added:" + url);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//		}
	}
	
	// if url starts with http/https
	boolean validUrl(String url){
		if (validHTTP(url)){
//			if (notXML(url)) {
//				return true;
//			}
            return true;
		}
		return false;

	}

	boolean notXML(String url){
		int i = url.lastIndexOf(";");
		String trueUrl = url.substring(0, i);
		if (trueUrl.endsWith(".xml")) return false;
		else return true;
	}

	boolean validHTTP(String url){
		return url.startsWith("https://") || url.startsWith("HTTPS://") || url.startsWith("http://") || url.startsWith("HTTP://");
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Do nothing
		this.collector = collector;
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
		this.collector.setRouter(router);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}
}
