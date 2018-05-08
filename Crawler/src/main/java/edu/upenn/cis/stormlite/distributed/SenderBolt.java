package edu.upenn.cis.stormlite.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

/**
 * This is a virtual bolt that is used to route data to the WorkerServer
 * on a different worker.
 * 
 * @author zives
 *
 */
public class SenderBolt implements IRichBolt {

        static Logger log = Logger.getLogger(SenderBolt.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
        Fields schema = new Fields("key", "value");
        
        String stream;
        String address;
        ObjectMapper mapper = new ObjectMapper();
        URL url;
        
        TopologyContext context;
        
        boolean isEndOfStream = false;
        

    public SenderBolt(String address, String stream) {
        this.stream = stream;
        this.address = address;
    }
    
        /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
                TopologyContext context, OutputCollector collector) {
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        this.context = context;
                try {
                        url = new URL(address + "/push/" + stream);
                } catch (MalformedURLException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Unable to create remote URL");
                }
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
                try {
                                send(input);
                        } catch (IOException e) {
                                e.printStackTrace();
                        }
    }
    
    /**
     * Sends the data along a socket
     * 
     *
     * @param tuple
     * @throws IOException 
     */
    private synchronized void send(Tuple tuple) throws IOException {
        isEndOfStream = tuple.isEndOfStream();
        
        log.debug("Sender is routing " + tuple.toString() + " to " + address + "/" + stream);
        
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestProperty("Content-Type", "application/json");
        conn.addRequestProperty("Connection", "close");
        String jsonForTuple = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(tuple);
        
        // send this to /push/{stream} as a POST!
        String reqType = "POST";
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);
		
		OutputStream os = null;
        try{
		    os = conn.getOutputStream();
        } catch (BindException be){
		    System.out.println("running out of local ports");
        }

		byte[] toSend = jsonForTuple.getBytes();
		os.write(toSend);
		os.flush();

		int i = conn.getResponseCode();

        conn.disconnect();
    }
   

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our executor/operator's unique ID
     */
        @Override
        public String getExecutorId() {
                return executorId;
        }

        /**
         * Called during topology setup, sets the router to the next
         * bolt
         */
        @Override
        public void setRouter(StreamRouter router) {
                // NOP for this, since its destination is a socket
        }

        /**
         * The fields (schema) of our output stream
         */
        @Override
        public Fields getSchema() {
                return schema;
        }
}
