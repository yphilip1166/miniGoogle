package edu.upenn.cis.stormlite.spout;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.mapreduce.worker.WorkerServerInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Simple word spout, largely derived from
 * https://github.com/apache/storm/tree/master/examples/storm-mongodb-examples
 * but customized to use a file called words.txt.
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
public abstract class FileSpout implements IRichSpout {

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
	 * This is a simple file reader
	 */
	String filename;
    BufferedReader reader;
	Random r = new Random();
	ArrayDeque<File> files = new ArrayDeque<>();
	
	int inx = 0;
	boolean sentEof = false;
	
    public FileSpout() {
    	filename = getFilename();
    }
    
    public abstract String getFilename();


    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // find input directory
        String inputDir = System.getProperty("user.dir") + "/" + WorkerServerInfo.getStore() + "/" + conf.get("inputDir");
        System.out.println("Input dir is:" + inputDir);
    	
        // open a directory and read all files
        File dir = new File(inputDir);
    	if (!dir.exists()){
    		System.out.println("Creating input directory");
    		dir.mkdir();
    	}
    	File[] fileList = dir.listFiles();
    	
    	// record all files
    	for (File f: fileList){
//    		System.out.println(f.toPath());
    		files.add(f);
    	}
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	if (reader != null)
	    	try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }

    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     */
    @Override
    public synchronized void nextTuple() {
    	if ((reader != null || !files.isEmpty()) && !sentEof) {
	    	try {
	    		// init reader 
	    		if (reader == null) {
	    			reader = new BufferedReader(new FileReader(files.pollFirst()));
	    		}
		    	String line = reader.readLine();
		    	if (line != null) {
		        	this.collector.emit(new Values<Object>(String.valueOf(inx++), line));
		        	// @piazza 1009
		        	WorkerServerInfo.incrementKeysRead();
		    	} else if (!files.isEmpty()){
		    		// read next file
		    		reader = new BufferedReader(new FileReader(files.pollFirst()));
		    	} else {
//		        	log.info(getExecutorId() + " finished file " + getFilename() + " and emitting EOS");
		    		// end of all files
			        this.collector.emitEndOfStream();
			        sentEof = true;
		    	}
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}
    	} else if (!sentEof){
    		this.collector.emitEndOfStream();
	        sentEof = true;
    	}
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
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
