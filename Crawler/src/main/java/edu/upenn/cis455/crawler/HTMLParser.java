package edu.upenn.cis455.crawler;

import java.util.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class HTMLParser {
	// parse html to list of urls
	public static List<String> parse(String content){
		try {
			List<String> toReturn = new ArrayList<String>();
			Document doc = Jsoup.parse(content);
			
			for (Element e: doc.select("a[href")) {
				toReturn.add(e.attr("href"));
			}
			
			return toReturn;
		} catch (Exception e){
			return new ArrayList<String>();
		}
		
	}
}
