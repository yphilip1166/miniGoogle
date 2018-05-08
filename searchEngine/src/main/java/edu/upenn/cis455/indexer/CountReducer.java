package edu.upenn.cis455.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.upenn.cis455.writable.CountWritable;
import edu.upenn.cis455.writable.HitWritable;
import edu.upenn.cis455.writable.WordUrlWritable;

public class CountReducer extends Reducer<WordUrlWritable, HitWritable, WordUrlWritable, CountWritable>{

	private MultipleOutputs<WordUrlWritable, CountWritable> multipleOutputs;
	
	public void reduce(WordUrlWritable wordUrlKey, Iterable<HitWritable> hitValues, Context context) throws IOException, InterruptedException {
		
		int termFreq = 0, capitalCnt = 0, titleCnt = 0, linkCnt = 0, emphaCnt = 0, metaCnt = 0;
		float headingScore = 0.0f, posScore = 0.0f;
		
		ArrayList<Integer> posList = new ArrayList();
		
		for(HitWritable hit : hitValues) {
			termFreq++;
			if(hit.getIsCapitalBoolean()) capitalCnt++;
			if(hit.getIsTitleBoolean()) titleCnt++;
			if(hit.getIsWithLinkBoolean()) linkCnt++;
			if(hit.getIsEmphasisBoolean()) emphaCnt++;
			if(hit.getIsMetaTagsBoolean()) metaCnt++;
			
			switch(hit.getHeadingNoInt()) {
			case 0: break;
			case 1: { headingScore += 1.0f; break; }
			case 2: { headingScore += 0.8f; break; }
			case 3: { headingScore += 0.6f; break; }
			case 4: { headingScore += 0.5f; break; }
			case 5: { headingScore += 0.4f; break; }
			case 6: { headingScore += 0.3f; break; }
			}
			
			if(hit.getPositionInt() < 500) posScore += 0.8f;
			else if (hit.getPositionInt() < 1000) posScore += 0.5f;
			else posScore += 0.2f;
			
			posList.add(hit.getPositionInt());
			
		}
		
		float termFreqFloat = (float) termFreq;
		float capitalPer = capitalCnt / termFreqFloat;
		float titlePer = titleCnt / termFreqFloat;
		float linkPer = linkCnt / termFreqFloat;
		float emphaPer = emphaCnt / termFreqFloat;
		float metaPer = metaCnt / termFreqFloat;
		
		Collections.sort(posList);
		int pos1 = (posList.size() > 0) ? posList.get(0) : -1;
		int pos2 = (posList.size() > 1) ? posList.get(1) : -1;
		int pos3 = (posList.size() > 2) ? posList.get(2) : -1;
		int pos4 = (posList.size() > 3) ? posList.get(3) : -1;
		int pos5 = (posList.size() > 4) ? posList.get(4) : -1;
		CountWritable count = new CountWritable(termFreq, capitalPer, titlePer, linkPer, emphaPer, metaPer, headingScore, posScore, pos1, pos2, pos3, pos4, pos5);
		
		char firstLetter = wordUrlKey.getWord().toString().charAt(0);
		if(Character.isLetter(firstLetter)) {
			multipleOutputs.write(Character.toString(firstLetter).toLowerCase(), wordUrlKey, count);
		}
		else if(Character.isDigit(firstLetter)) {
			multipleOutputs.write("number", wordUrlKey, count);
		}
	}
	
	@Override
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<WordUrlWritable, CountWritable>(context);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
