package edu.upenn.cis455.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CountWritable implements Writable {

	private IntWritable termFreq = new IntWritable();
	private FloatWritable capitalPer = new FloatWritable();
	private FloatWritable titlePer = new FloatWritable();
	private FloatWritable linkPer = new FloatWritable();
	private FloatWritable emphaPer = new FloatWritable();
	private FloatWritable metaPer = new FloatWritable();
	
	private FloatWritable headingScore = new FloatWritable();
	private FloatWritable positionScore = new FloatWritable();
	
	private IntWritable pos1 = new IntWritable();
	private IntWritable pos2 = new IntWritable();
	private IntWritable pos3 = new IntWritable();
	private IntWritable pos4 = new IntWritable();
	private IntWritable pos5 = new IntWritable();
	
	
	public CountWritable() {
		termFreq.set(0); 
		capitalPer.set(0);
		titlePer.set(0);
		linkPer.set(0);
		emphaPer.set(0);
		metaPer.set(0);
		headingScore.set(0.0f);
		positionScore.set(0.0f);
		pos1.set(-1);pos2.set(-1);pos3.set(-1);pos4.set(-1);pos5.set(-1);
	}
	
	public CountWritable(IntWritable termFreq, FloatWritable capitalPer, FloatWritable titlePer, 
			FloatWritable linkPer, FloatWritable emphaPer, FloatWritable metaPer,
			FloatWritable headingScore, FloatWritable positionScore, IntWritable pos1, 
			IntWritable pos2, IntWritable pos3, IntWritable pos4, IntWritable pos5) {
		super();
		this.termFreq = termFreq;
		this.capitalPer = capitalPer;
		this.titlePer = titlePer;
		this.linkPer = linkPer;
		this.emphaPer = emphaPer;
		this.metaPer = metaPer;
		this.headingScore = headingScore;
		this.positionScore = positionScore;
		this.pos1 = pos1; this.pos3 = pos3; this.pos3 = pos3; this.pos4 = pos4; this.pos5 = pos5;
	}

	public CountWritable(int termFreq, float capitalPer, float titlePer, float linkPer, float emphaPer, 
			float metaPer, float headingScore, float positionScore, int pos1, int pos2, int pos3, int pos4, int pos5) {
		super();
		this.termFreq.set(termFreq);
		this.capitalPer.set(capitalPer);
		this.titlePer.set(titlePer);
		this.linkPer.set(linkPer);
		this.emphaPer.set(emphaPer);
		this.metaPer.set(metaPer);
		this.headingScore.set(headingScore);
		this.positionScore.set(positionScore);
		this.pos1.set(pos1);this.pos2.set(pos2);this.pos3.set(pos3);this.pos4.set(pos4);this.pos5.set(pos5);
	}

	@Override
	public String toString() {
		return termFreq + "\t" + capitalPer + "\t" + titlePer + "\t" + linkPer + "\t" + emphaPer + "\t"
				+ metaPer+ "\t" + headingScore + "\t" + positionScore + "\t" + pos1 + "\t" + pos2 + "\t" + pos3 
				+ "\t" + pos4 + "\t" + pos5;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
