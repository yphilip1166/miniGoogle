package edu.upenn.cis455.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class UrlHitWritable implements Writable {

	private Text url;
	private HitWritable hit;
	
	
	public UrlHitWritable() {
		this.url = new Text();
		this.hit = new HitWritable();
	}
	
	public UrlHitWritable(Text url, HitWritable hit) {
		super();
		this.url = url;
		this.hit = hit;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		url.readFields(in);
		hit.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		url.write(out);
		hit.write(out);
	}

	@Override
	public String toString() {
		return url.toString() + "; " + hit.toString();
	}
	
	/**
	 * Getters and Setters
	 */
	public Text getUrl() {
		return url;
	}

	public void setUrl(Text url) {
		this.url = url;
	}

	public HitWritable getHit() {
		return hit;
	}

	public void setHit(HitWritable hit) {
		this.hit = hit;
	}

}
