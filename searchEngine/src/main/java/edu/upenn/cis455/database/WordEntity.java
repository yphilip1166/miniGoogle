package edu.upenn.cis455.database;

import java.util.Arrays;
import java.util.HashMap;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class WordEntity {
	
	@PrimaryKey
	private String stemmedWord;
	
	// <url, [tf, zoneScore, posScore, existUppercase,...]
	private HashMap<String, float[]> scoreMap = new HashMap<>();
	
	public WordEntity(){}
	public WordEntity(String word) {
		this.stemmedWord = word;
	}
	
	public void addUrlEntry(String line) {
		//System.out.println(line);
		
		String[] strs = line.split("\t");
		String url = strs[0];
		int TF = strs.length - 1;
		
		boolean inTitle = false;
		boolean inBody = false;
		float hasUppercase = 0.0f;
		float posScore = 0.0f;
		for(int i=1; i <= TF; i++) {
			String hit = strs[i];
			String[] parts = hit.split(";");
			
			if(parts[0].equals("1")) hasUppercase = 1.0f;
				
			if(parts[1].equals("1")) inTitle = true;
			else inBody = true;
			
			int pos = Integer.parseInt(parts[2]);
			if(pos < 500) posScore += 0.5f;
			else if(pos < 1000) posScore += 0.3f;
			else posScore += 0.2f;
		}
		
		float zoneScore = 0.0f, g = 0.6f;
		if(inTitle) zoneScore += g;
		if(inBody) zoneScore += (1.0-g);
		
		float[] score = new float[]{TF, zoneScore, posScore, hasUppercase};
		scoreMap.put(url, score);
	}
	
	public float[] getScore(String url) {
		return scoreMap.get(url);
	}
	
	public float getIDF(int N) {
		int DF = scoreMap.size();
		float IDF = (float) Math.log(N/DF);
		return IDF;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(stemmedWord).append("\n");
		for(String url : scoreMap.keySet()) {
			sb.append(url).append(": ");
			sb.append(Arrays.toString(scoreMap.get(url))).append("\n");
		}
		return sb.toString();
	}
	
}
