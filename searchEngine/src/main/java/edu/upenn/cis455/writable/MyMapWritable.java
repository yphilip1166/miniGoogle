package edu.upenn.cis455.writable;

import org.apache.hadoop.io.MapWritable;

public class MyMapWritable extends MapWritable {
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("\n");
		for(Object key: this.keySet()) {
			sb.append(key.toString()).append("\t");
			HitListWritable hitList = (HitListWritable) this.get(key); 
			sb.append(hitList.toString()).append("\n");
		}
		return sb.toString();
	}
}
