package edu.upenn.cis455.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordUrlWritable implements WritableComparable<WordUrlWritable> {
	
	private Text word = new Text();
	private Text url = new Text();
	
	public WordUrlWritable() {};
	
	public WordUrlWritable(Text word, Text url) {
		super();
		this.word = word;
		this.url = url;
	}
	
	public WordUrlWritable(String word, String url) {
		super();
		this.word.set(word);
		this.url.set(url);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		url.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		url.write(out);
	}
	
	@Override
	public String toString() {
		return word.toString() + "\t" + url.toString();
	}

	public Text getWord() {
		return word;
	}
	
	public void setWord(Text word) {
		this.word = word;
	}
	
	public Text getUrl() {
		return url;
	}
	
	public void setUrl(Text url) {
		this.url = url;
	}

	@Override
	public int compareTo(WordUrlWritable other) {
		int cmp = word.compareTo(other.word);
		if(cmp != 0) return cmp;
		else return url.compareTo(other.url);
	}
	
}
