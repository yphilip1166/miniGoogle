package edu.upenn.cis455.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


public class PerUrlRecordReader extends RecordReader<Text, Text> {

	private static Logger log = Logger.getLogger(PerUrlRecordReader.class);
	
	private FileSplit fileSplit;
	private Configuration conf;
	private Text urlKey = new Text();
	private Text docValue = new Text();
	private boolean isDone = false;
	
	private BufferedReader reader;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		fileSplit = (FileSplit) split;
		conf = context.getConfiguration();
		log.debug("initializer PerUrlRecordReader");
		
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(conf);
		reader = new BufferedReader(new InputStreamReader(fs.open(file)));
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!isDone) {
			String urls = reader.readLine();
			if(urls == null) {
				isDone = true;
				return false;
			}
				
			String url = urls.split(",")[0];
			urlKey.set(url);
				
			StringBuilder sb = new StringBuilder();
			String line = reader.readLine();
			while(line != null && !line.equals("**************** Linghan Split cis 555 ***************")) {
				sb.append(line).append("\n");
				line = reader.readLine();
			}
			
			if(line == null) {
				isDone = true; return true;
			}
			
			docValue.set(sb.toString());
			//System.out.println(docValue);
			
			return true;
		}
		return false;
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return urlKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return docValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return isDone ? 1.0f : 0.0f;
	}
	
	@Override
	public void close() throws IOException {
		log.debug("WholeFileRecordReader close");
		reader.close();
	}
	
}
