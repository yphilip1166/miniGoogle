package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayDeque;

public class ArrayHelper {
	
	// helper class to convert string to array and back
	
	public static String convertToString(ArrayDeque<String> results){
		StringBuilder sb = new StringBuilder("[");
		for (String result: results){
			sb.append(result).append(",");
		}
		if (sb.charAt(sb.length()-1) == ','){
			sb.deleteCharAt(sb.length()-1);
		}
		sb.append("]");
		return sb.toString();
	}
	
	public static String[] convertToArray(String results){
		results = results.substring(1, results.length()-1);
		String[] toReturn = results.split(",");
		return toReturn;
	}
}
