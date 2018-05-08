package edu.upenn.cis455.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class HitWritable implements Writable {

	private BooleanWritable isCapital = new BooleanWritable();
	
	private BooleanWritable isTitle = new BooleanWritable();
	private BooleanWritable isWithLink = new BooleanWritable();
	private BooleanWritable isEmphasis = new BooleanWritable();
	private BooleanWritable isMetaTags = new BooleanWritable();
	
	private IntWritable headingNo = new IntWritable();
	private IntWritable position = new IntWritable();
	
	
	public HitWritable() {
		isCapital.set(false);
		isTitle.set(false);
		isWithLink.set(false);
		isEmphasis.set(false);
		isMetaTags.set(false);
		headingNo.set(0);
		position.set(-1);
	}
	
	public HitWritable(boolean isCapital, boolean isTitle, boolean isWithLink, boolean isEmphasis, 
				boolean isMetaTags, int headingNo, int position) {
		super();
		this.isCapital.set(isCapital);
		this.isTitle.set(isTitle);
		this.isWithLink.set(isWithLink);
		this.isEmphasis.set(isEmphasis);
		this.isMetaTags.set(isMetaTags);
		this.headingNo.set(headingNo);
		this.position.set(position);
	}
	
	public HitWritable(BooleanWritable isCapital, BooleanWritable isTitle, BooleanWritable isWithLink, 
			BooleanWritable isEmphasis, BooleanWritable isMetaTags, IntWritable headingNo, IntWritable position) {
		super();
		this.isCapital = isCapital;
		this.isTitle = isTitle;
		this.isWithLink = isWithLink;
		this.isEmphasis = isEmphasis;
		this.isMetaTags = isMetaTags;
		this.headingNo = headingNo;
		this.position = position;
	}
	
	public HitWritable(HitWritable other) {
		this.isCapital = new BooleanWritable(other.getIsCapitalBoolean());
		this.isTitle = new BooleanWritable(other.getIsTitleBoolean());
		this.position = new IntWritable(other.getPositionInt());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		isCapital.readFields(in);
		isTitle.readFields(in);
		isWithLink.readFields(in);
		isEmphasis.readFields(in);
		isMetaTags.readFields(in);
		headingNo.readFields(in);
		position.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		isCapital.write(out);
		isTitle.write(out);
		isWithLink.write(out);
		isEmphasis.write(out);
		isMetaTags.write(out);
		headingNo.write(out);
		position.write(out);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(isCapital.get() ? "1" : "0");
		sb.append(";");
		sb.append(isTitle.get() ? "1" : "0");
		sb.append(";");
		sb.append(isWithLink.get() ? "1" : "0");
		sb.append(";");
		sb.append(isEmphasis.get() ? "1" : "0");
		sb.append(";");
		sb.append(isMetaTags.get() ? "1" : "0");
		sb.append(";");
		sb.append(headingNo.get()).append(";");
		sb.append(position.get());
		return sb.toString();
	}

	/**
	 * Getters and Setters
	 */
	// isCapital
	public BooleanWritable getIsCapital() {
		return isCapital;
	}
	
	public boolean getIsCapitalBoolean() {
		return isCapital.get();
	}

	public void setIsCapital(BooleanWritable isCapital) {
		this.isCapital = isCapital;
	}
	
	public void setIsCapital(boolean isC) {
		this.isCapital.set(isC);
	}

	// isTitle
	public BooleanWritable getIsTitle() {
		return isTitle;
	}
	
	public boolean getIsTitleBoolean() {
		return isTitle.get();
	}

	public void setIsTitle(BooleanWritable isTitle) {
		this.isTitle = isTitle;
	}
	
	public void setIsTitle(boolean isT) {
		this.isTitle.set(isT);
	}

	// isWithLink
	public BooleanWritable getIsWithLink() {
		return isWithLink;
	}

	public boolean getIsWithLinkBoolean() {
		return isWithLink.get();
	}
	
	public void setIsWithLink(BooleanWritable isWithLink) {
		this.isWithLink = isWithLink;
	}
	
	public void setIsWithLink(boolean isWithLink) {
		this.isWithLink.set(isWithLink);
	}
	
	// isEmphasis
	public BooleanWritable getIsEmphasis() {
		return isEmphasis;
	}

	public boolean getIsEmphasisBoolean() {
		return isEmphasis.get();
	}
	
	public void setIsEmphasis(BooleanWritable isEmphasis) {
		this.isEmphasis = isEmphasis;
	}
	
	public void setIsEmphasis(boolean isEmphasis) {
		this.isEmphasis.set(isEmphasis);
	}

	// isMetaTags
	public BooleanWritable getIsMetaTags() {
		return isMetaTags;
	}

	public boolean getIsMetaTagsBoolean() {
		return isMetaTags.get();
	}
	
	public void setIsMetaTags(BooleanWritable isMetaTags) {
		this.isMetaTags = isMetaTags;
	}
	
	public void setIsMetaTags(boolean isMetaTags) {
		this.isMetaTags.set(isMetaTags);
	}

	// headingNumber
	public IntWritable getHeadingNo() {
		return headingNo;
	}
	
	public int getHeadingNoInt() {
		return headingNo.get();
	}

	public void setHeadingNo(IntWritable headingNo) {
		this.headingNo = headingNo;
	}
	
	public void setHeadingNoInt(int headingNo) {
		this.headingNo.set(headingNo);
	}
	

	// position
	public IntWritable getPosition() {
		return position;
	}
	
	public int getPositionInt() {
		return position.get();
	}

	public void setPosition(IntWritable position) {
		this.position = position;
	}

	public void setPosition(int pos) {
		this.position.set(pos);
	}
}
