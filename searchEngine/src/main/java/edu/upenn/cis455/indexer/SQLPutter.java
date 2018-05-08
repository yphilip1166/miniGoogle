package edu.upenn.cis455.indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.upenn.cis455.database.MySQLWrapper;

public class SQLPutter {

	public static void main(String[] args) {
		
		if(args.length < 1) {
			 System.out.println("Usage: [MapReduceOutputPath]");
			 return;
		}
		
		MySQLWrapper sql = new MySQLWrapper();
		sql.Initialize();
		
		Configuration conf = new Configuration();
		
		Path filePath = new Path(args[0]);
		FileSystem fs;
		try {
			fs = filePath.getFileSystem(conf);
			FileStatus[] status = fs.listStatus(filePath);
			
			for(FileStatus file : status) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
				String line = reader.readLine();
				while(line != null) {
					String[] subStr = line.split("\t");
					String[] subsubStr = subStr[2].split(";");
					
					String word = subStr[0];
					String url = subStr[1];
                    int tf = Integer.parseInt(subsubStr[0]);
                    float capitalPercent = Float.parseFloat(subsubStr[1]);
                    float titlePercent = Float.parseFloat(subsubStr[2]);
                    float linkPercent = Float.parseFloat(subsubStr[3]);
                    float emphasisPercent = Float.parseFloat(subsubStr[4]);
                    float metaPercent = Float.parseFloat(subsubStr[5]);
                    float headingScore = Float.parseFloat(subsubStr[6]);
                    float positionScore = Float.parseFloat(subsubStr[7]);
                    
                    sql.putRecord(word, url, tf, capitalPercent, titlePercent, linkPercent, emphasisPercent, 
                    		metaPercent, headingScore, positionScore);
                    
                    line = reader.readLine();
				}
				reader.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
