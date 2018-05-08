package edu.upenn.cis455.indexer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.upenn.cis455.io.PerUrlInputFormat;
import edu.upenn.cis455.writable.CountWritable;
import edu.upenn.cis455.writable.HitWritable;
import edu.upenn.cis455.writable.WordUrlWritable;

public class IndexerMain extends Configured implements Tool {
	
	public static Logger log = Logger.getLogger(IndexerMain.class);

	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2) {
			 System.out.println("Usage: [inputPath] [outputPath]");
			 return 2;
		}
		
		// First, MapReduce construct inverted index
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "IndexerMain");
		
		job.getConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
		job.getConfiguration().set("fs.s3a.access.key", "AKIAJUS47FJ6Z7NT66SA");
        job.getConfiguration().set("fs.s3a.secret.key","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");
        job.getConfiguration().set("mapreduce.task.timeout", "6000000");
        
		job.setJarByClass(IndexerMain.class);
		job.setMapperClass(HitConstructMapper.class);
		job.setInputFormatClass(PerUrlInputFormat.class);
		job.setMapOutputKeyClass(WordUrlWritable.class);
		job.setMapOutputValueClass(HitWritable.class);
		
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(WordUrlWritable.class);
		job.setOutputValueClass(CountWritable.class);
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		for(char ch = 'a'; ch <= 'z'; ch++)
			MultipleOutputs.addNamedOutput(job, Character.toString(ch), TextOutputFormat.class, WordUrlWritable.class, CountWritable.class);
		MultipleOutputs.addNamedOutput(job, "number", TextOutputFormat.class, WordUrlWritable.class, CountWritable.class);
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		// Delete output filepath if already exists
		FileSystem fs = outputFilePath.getFileSystem(conf);
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new IndexerMain(), args);
		System.exit(res);
	}
}
