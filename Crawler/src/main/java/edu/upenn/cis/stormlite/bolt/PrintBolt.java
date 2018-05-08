package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.worker.WorkerServerInfo;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.UUID;

/**
 * A trivial bolt that simply outputs its input stream to the
 * console
 * 
 * @author zives
 *
 */
public class PrintBolt implements IRichBolt {
	static Logger log = Logger.getLogger(PrintBolt.class);
	
	Fields myFields = new Fields();

	File file;
	
	int neededVotesToComplete = 0;
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
		if (!input.isEndOfStream()){
			System.out.println(getExecutorId() + ": " + input.toString());
			try {
				// write input to output.txt (over write content in output)
				PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file.toString(), true)));
				String key = input.getStringByField("key");
		        String value = input.getStringByField("value");
		        // add result for status
		        WorkerServerInfo.addResult(key+":"+value);
				pw.println(key+","+value);
				pw.close();
			} catch (IOException e){
				System.out.println(e);
			}
		} 
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Open directory and create file
		String dir = System.getProperty("user.dir") + "/" + WorkerServerInfo.getStore() + "/" + stormConf.get("outputDir");
		System.out.println("Output dir is:" + dir);
		File outputDir = new File(dir);
		if (!outputDir.exists()) {
			outputDir.mkdir();
		}
		
		// create file
		String filePath = dir + "/output.txt";
		file = new File(filePath);
		if (file.exists()){
			PrintWriter writer;
			try {
				// clear file
				writer = new PrintWriter(file);
				writer.print("");
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
		}
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
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
