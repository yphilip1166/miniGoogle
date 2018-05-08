package edu.upenn.cis455.mapreduce.job;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

import java.util.Iterator;

public class WordCount implements Job {

  public void map(String key, String value, Context context)
  {
    // Your map function for WordCount goes here
	  // split the line and emit each word
	  String[] words = value.split("[ \\t\\,.]");
	  for (String word: words){
//		  System.out.println(word);
		  context.write(word, "1");
	  }
  }
  
  public void reduce(String key, Iterator<String> values, Context context)
  {
    // Your reduce function for WordCount goes here
	  // for each of the words, count the number and emit
	  int count = 0;
	  while (values.hasNext()){
		  count+= Integer.parseInt(values.next());
	  }
	  context.write(key, count + "");
  }
  
}
