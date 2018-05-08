package edu.upenn.cis455.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;

public class HitListWritable implements Writable {

	private ArrayList<HitWritable> hitList;
	
	public HitListWritable() {
		super();
		hitList = new ArrayList<HitWritable>();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		hitList = new ArrayList<HitWritable>();
		for (int i=0; i<size; i++) {
			HitWritable hit = new HitWritable();
			hit.readFields(in);
			hitList.add(hit);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(hitList.size());
		for(HitWritable hit : hitList) {
			hit.write(out);
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(HitWritable hit : hitList) {
			sb.append(hit.toString()).append("\t");
		}
		return sb.toString();
	}
	
	public void add(HitWritable hit) {
		hitList.add(hit);
	}
	
	public int size() {
		return hitList.size();
	}

}
