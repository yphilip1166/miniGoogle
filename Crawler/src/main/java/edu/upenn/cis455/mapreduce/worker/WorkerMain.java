package edu.upenn.cis455.mapreduce.worker;

import java.util.HashMap;
import java.util.Map;
import java.io.FileReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.IOException;

public class WorkerMain {
    public static void main(String[] args) throws IOException{
    	
    	// start a worker
    	if (args.length != 4) {
    		System.out.println("Needed four arguments: master address, node directory, port number, worker index");
    		System.exit(0);
    	}
        Map<String, String> config = new HashMap<String, String>();

        config.put("masterDir", args[0]);
        String dir = args[1];
        if (dir.startsWith("~")){
        	dir = dir.substring(1);
        }

        //set s3 key and id
        FileReader f = null;
        try {
            f = new FileReader(new File("./conf/aws"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader bf = new BufferedReader(f);
        String key = bf.readLine();
        String id = bf.readLine();
        System.out.println("key: "+key);
        System.out.println(("id: "+id));
        //System.setProperty("KEY", key);
        //System.setProperty("ID", id);

        config.put("storeDir", dir);
        config.put("port", args[2]);
        config.put("workerIdx", args[3]);
        config.put("awsKey", key);
        config.put("awsID", id);
        WorkerServer.createWorker(config);
    }
}
