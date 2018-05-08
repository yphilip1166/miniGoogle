package edu.upenn.cis.stormlite.spout;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.URLWithData;
import edu.upenn.cis455.crawler.XPathCrawlerInfo;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

public class CrawlerSpout implements IRichSpout {
	static Logger log = Logger.getLogger(CrawlerSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
	SpoutOutputCollector collector;

	/**
	 * This is a simple file reader for words.txt
	 */

    public CrawlerSpout() {
    	log.debug("Starting spout");
    }


    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    }

    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     */
    @Override
	public void nextTuple() {
//		System.out.println("calling nextTuple in crawlerspout");
		try {
			// check robot first
			URLWithData url_la = XPathCrawlerInfo.getURL();

			String url = url_la.getURL();
			String la = url_la.getLastAccessedTime();
			String fromUrl = url_la.getFromUrl();

			int result = XPathCrawlerInfo.passRobot(url);
			if (result == 1){
//				System.out.println("In Spout emitting:" +url+ " " +la);
                URL temp = new URL(url);
                String host = temp.getHost();
				this.collector.emit(new Values<Object>(url, la, host, fromUrl));
			} else if (result == 0){
				XPathCrawlerInfo.putURL(url_la);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.debug("Error in getting url from queue");
			Thread.yield();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			Thread.yield();
		} catch (NumberFormatException e){
			Thread.yield();
		}

		Thread.yield();
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("SpoutURL", "LastAccessed", "HostName", "FromURL"));
    }


	@Override
	public String getExecutorId() {
		return executorId;
	}


	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

}

