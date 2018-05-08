package edu.upenn.cis455.indexer;

import java.io.BufferedReader;
import java.io.File;

import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;

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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.upenn.cis455.io.PerUrlInputFormat;
import edu.upenn.cis455.writable.CountWritable;
import edu.upenn.cis455.writable.HitWritable;
import edu.upenn.cis455.writable.WordUrlWritable;

public class IndexerMergeMain extends Configured implements Tool {
	
	public static Logger log = Logger.getLogger(IndexerMergeMain.class);

	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 3) {
			 System.out.println("Usage: [inputPath:\"input/subinput\"] [mergePath:\"s3://...\"] [outputPath:\"s3://...\"]");
			 return 3;
		}
		
		String[] splitArg1 = args[1].split("/");
		String mergeFileFolder = splitArg1[splitArg1.length-1] + "/";
		
		// First, MapReduce construct inverted index
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "IndexerMain");
		
		job.getConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
		job.getConfiguration().set("fs.s3a.access.key", "AKIAJUS47FJ6Z7NT66SA");
        job.getConfiguration().set("fs.s3a.secret.key","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");
        
		job.setJarByClass(IndexerMergeMain.class);
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
		
		Path mergeFilePath = new Path(args[1]);
		Path outputFilePath = new Path(args[2]);
		
		// Merge Files
		AWSCredentials credentials = new BasicAWSCredentials("AKIAJUS47FJ6Z7NT66SA", "ruqGHmp37HqUyabDTw30VwGzJmJbxrjEJNUcWsdf");
		AmazonS3 s3client = new AmazonS3Client(credentials);
        
        ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName("cis555-database").withPrefix(args[0]);
        ObjectListing objectListing = s3client.listObjects(listObjectRequest);
        List<S3ObjectSummary> s3ObjectSummaries = objectListing.getObjectSummaries();
        while (objectListing.isTruncated()) 
        {
        	objectListing = s3client.listNextBatchOfObjects (objectListing);
           	s3ObjectSummaries.addAll (objectListing.getObjectSummaries());
        }
        
        int i = 0;
        int iter = 0;
        String CurrFileName = "output"+String.valueOf(iter)+".txt";
        
        PrintWriter pw = new PrintWriter(CurrFileName);
        for(S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
        	//System.out.println(i);
        	String keyName = s3ObjectSummary.getKey();
        	
        	S3Object object = s3client.getObject(new GetObjectRequest("cis555-database", keyName));
        	BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        	
        	//System.out.println("keyName:" + keyName);
        	String line = reader.readLine();
        	while(line != null) {
        		//System.out.println(line);
        		pw.append(line+"\n");
        		line = reader.readLine();
        	}
        	
        	pw.append("**************** Linghan Split cis 555 ***************\n");
        	reader.close();
        	
        	// every 1000 file -> a big file, upload to S3
        	if(i % 1000 == 999 || i == s3ObjectSummaries.size()-1) {
        		pw.close();
        		
        		PutObjectRequest request = new PutObjectRequest("cis555-database", mergeFileFolder+"output"+String.valueOf(iter), new File(CurrFileName));
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentType("plain/text");
                metadata.addUserMetadata("x-amz-meta-title", "someTitle");
                request.setMetadata(metadata);
                s3client.putObject(request);
                iter++;
                
                File f = new File(CurrFileName);
                f.delete();
                
        		CurrFileName = "output"+String.valueOf(iter)+".txt";
        		pw = new PrintWriter(CurrFileName);
				System.out.println("merged until:" + i);
        	}
        	i++;
        }
        pw.close();
        
        // Real Run job
		FileInputFormat.addInputPath(job, mergeFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		// Delete output filepath if already exists
		FileSystem fs = outputFilePath.getFileSystem(conf);
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new IndexerMergeMain(), args);
		System.exit(res);
	}
}
