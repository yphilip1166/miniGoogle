package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServerInfo;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.UUID;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ReduceBolt implements IRichBolt {
	static Logger log = Logger.getLogger(ReduceBolt.class);
	
	
	Job reduceJob;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	boolean sentEof = false;
	
	/**
	 * Buffer for state, by key
	 */
//	Map<String, List<String>> stateByKey = new HashMap<>();
	private final Object dbLock = new Object();

	/**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    private TopologyContext context;
    
    int neededVotesToComplete = 0;
    
    public ReduceBolt() {
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;

        if (!stormConf.containsKey("reduceClass"))
        	throw new RuntimeException("Mapper class is not specified as a config option");
        else {
        	String mapperClass = stormConf.get("reduceClass");
        	
        	try {
				reduceJob = (Job)Class.forName(mapperClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException("Unable to instantiate the class " + mapperClass);
			}
        }
        if (!stormConf.containsKey("mapExecutors")) {
        	throw new RuntimeException("Reducer class doesn't know how many map bolt executors");
        }

        // determine how many EOS votes needed
        String numFileSpout = stormConf.get("spoutExecutors");
        String numMap = stormConf.get("mapExecutors");
        String numReduce = stormConf.get("reduceExecutors");
        
        // convert to int for calculation
        int intFileSpout = Integer.parseInt(numFileSpout);
        int intMap = Integer.parseInt(numMap);
        int intReduce = Integer.parseInt(numReduce);
        int intWorker = WorkerHelper.getWorkers(stormConf).length;
        neededVotesToComplete = (intWorker - 1) * intMap * intReduce + intMap;
    }

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized void execute(Tuple input) {
    	if (sentEof) {
	        if (!input.isEndOfStream())
	        	throw new RuntimeException("We received data after we thought the stream had ended!");
    		// Already done!
		} else if (input.isEndOfStream()) {
			
			//  only if at EOS do we trigger the reduce operation and output all state
			if (--neededVotesToComplete == 0){
				
				// emit all key value in db thats put in by this worker
//				if (DBWrapper.containsState(getExecutorId())){
//					for (Map.Entry<String, ArrayList<String>> entry: DBWrapper.getState(getExecutorId()).getMap().entrySet()){
//						String k = entry.getKey();
//						ArrayList<String> vs = entry.getValue();
//						reduceJob.reduce(k, vs.iterator(), this.collector);
//						WorkerServerInfo.incrementKeysRead();
//						WorkerServerInfo.incrementKeysWritten(vs.size());
//						WorkerServerInfo.setStatus("reducing");
//					}
//				}

				// end reducer
				sentEof = true;
				collector.emitEndOfStream();
				WorkerServerInfo.setStatus("IDLE");
			}
			
    	} else {
    		// this is a plain ol' hash map, replace it with BerkeleyDB
    		// store in db
    		String key = input.getStringByField("key");
	        String value = input.getStringByField("value");
	        log.debug(getExecutorId() + " received " + key + " / " + value);
	        
	        // db should be already synchronized 
	        synchronized (dbLock){
	        	State toAdd = new State();
	        	
//	        	if (!DBWrapper.containsState(getExecutorId())){
//	        		// @piazza 1009
//	        		toAdd.setID(getExecutorId());
//	        	} else {
//	        		toAdd = DBWrapper.getState(getExecutorId());
//	        	}
//	        	toAdd.put(key, value);
//	        	DBWrapper.putState(toAdd);
	        }
    	}        
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
     * Used for debug purposes, shows our exeuctor/operator's unique ID
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
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
