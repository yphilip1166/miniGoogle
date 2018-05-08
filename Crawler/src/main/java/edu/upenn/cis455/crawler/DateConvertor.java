package edu.upenn.cis455.crawler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateConvertor {
	// convert long date to date object
	public static String longToStr(long time){
		SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		String t = sdf.format(new Date(time));
		return t;
	}
}
