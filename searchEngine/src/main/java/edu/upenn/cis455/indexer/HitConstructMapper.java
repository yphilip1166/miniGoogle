package edu.upenn.cis455.indexer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.tokenize.SimpleTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis455.writable.HitWritable;
import edu.upenn.cis455.writable.WordUrlWritable;

public class HitConstructMapper extends Mapper<Text, Text, WordUrlWritable, HitWritable> {
	
	private static Logger log = Logger.getLogger(HitConstructMapper.class);
	
	private HashSet<String> stopwords = new HashSet<>(Arrays.asList("a", "about", "above", "after",
			"again", "against", "an", "am", "all", "and", "as", "are", "at", "be", "because", "been", 
			"being", "before", "but", "both", "by", "can", "could", "did", "do", "dose", "doing", "for",
			"have", "has", "had", "he", "her", "here", "him", "his", "how", "i", "in", "is", "it", 
			"its", "me", "my", "no", "nor", "of", "on", "or", "ought", "our", "ours", "she", "so", "some", 
			"such", "than", "that", "the", "their", "them", "then", "there", "this","those", "to", "too", 
			"very", "was", "we", "were", "what", "when", "which", "who", "whom", "why", "with", "would", 
			"you", "your", "yours", "b", "c", "d", "e", "f", "g", "h", "j", "k", "l", "m", "n", "o",
			"p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"));
	
	private HashSet<String> meaningfulNums = new HashSet<>(Arrays.asList("455", "555", "505", "76", "911",
			"666", "520", "007", "110", "120", "119", "108", "10", "100"));
	
	public void map(Text urlKey, Text docValue, Context context) throws IOException, InterruptedException {
		
		//System.out.println(docValue);
		Document document = Jsoup.parse(docValue.toString());
		Elements allElements = document.getAllElements();
		
		int pos = 0;
		for(Element element : allElements) {
			String nodeName = element.nodeName().toLowerCase().trim();
			
			// <META>
			if(nodeName.equals("meta") && 
					(element.attr("name").equals("description") || element.attr("name").equals("keywords"))) {
				
				String text = element.attr("content");
				pos = emit(urlKey, text, false, false, false, true, 0, pos, context);
			}
			
			// <TITLE>
			else if(nodeName.equals("title")) {
				
				String text = element.ownText();
				pos = emit(urlKey, text, true, false, false, false, 0, pos, context);
			}
			
			// Tag used to emphasize (<i>, <b>, <mark>, <em>, <strong>)
			else if(nodeName.equals("b") || nodeName.equals("i") || nodeName.equals("mark") ||
					nodeName.equals("em") || nodeName.equals("strong")) {
				
				String text = element.ownText();
				pos = emit(urlKey, text, false, false, true, false, 0, pos, context);
			}
			
			// Heading <h*>
			else if(nodeName.matches("h[1-6]")) {
				
				String text = element.ownText();
				pos = emit(urlKey, text, false, false, true, false, nodeName.charAt(1)-'0', pos, context);
			}
			
			// Link <a>
			else if(nodeName.matches("a")) {
				
				String text = element.ownText();
				pos = emit(urlKey, text, false, true, false, false, 0, pos, context);
			}
			
			else if(element.ownText()!=null && element.ownText().length()!=0) {
				
				String text = element.ownText();
				pos = emit(urlKey, text, false, false, false, false, 0, pos, context);
			}
		}
	}
	
	private int emit(Text urlKey, String text, boolean isTitle, boolean isWithLink, boolean isEmphasis, 
			boolean isMetaTags, int headingNo, int curPos, Context context) {
		
		//System.out.println(text);
		// Tokenizing using OpenNLP
		SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		String[] tokens = tokenizer.tokenize(text);
		
		for(String word : tokens) {
			word = word.trim();
			if(word.length() == 0) continue;
			// contain non-digit/character, or is stopword, skip
			if(!word.matches("[a-zA-Z0-9]*") || stopwords.contains(word.toLowerCase())) continue;
			
			if(word.matches("[0-9]*")) {
				if(word.length() > 4) continue;
				int number = Integer.parseInt(word);
				if(number >= 2100) continue;
				if(number < 1000 && !meaningfulNums.contains(word)) continue;
				
				if(!meaningfulNums.contains(word))
					word = String.valueOf(Integer.parseInt(word));
			}
			
			String upperWord = word.toUpperCase();
			boolean isCapital = word.equals(upperWord);
			
			HitWritable hit = new HitWritable(isCapital, isTitle, isWithLink, isEmphasis, isMetaTags, headingNo, curPos++);
			
			// Stemming using OpenNLP
			PorterStemmer stemmer = new PorterStemmer();
			if(isCapital) {
				String stemmedWord = stemmer.stem(word);
				if(stemmedWord.length() > 32) continue;
				WordUrlWritable wordUrl = new WordUrlWritable(new Text(stemmedWord), urlKey);
				log.debug(wordUrl.toString() + " -> " + hit.toString());
				try {
					context.write(wordUrl, hit);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
					continue;
				}
			} else {
				String stemmedWord = stemmer.stem(word.toLowerCase());
				if(stemmedWord.length() > 32) continue;
				WordUrlWritable wordUrl = new WordUrlWritable(new Text(stemmedWord), urlKey);
				log.debug(wordUrl.toString() + " -> " + hit.toString());
				try {
					context.write(wordUrl, hit);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
					continue;
				}
			}
		
		}
		
		return curPos;
	}
	
}
