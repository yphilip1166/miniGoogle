package edu.upenn.cis455.crawler;

import edu.upenn.cis455.crawler.info.RobotsTxtInfo;

public class RobotParser {
	
	// parse a robot file into robottxtinfo class
	public static RobotsTxtInfo parse(String content){
		
		if (content == null) return null;
		//System.out.println(content);
		String cUser = null;
		RobotsTxtInfo rti = new RobotsTxtInfo();
		String lineSep = System.getProperty("line.separator");
		
		// parse each line
		for (String line: content.split(lineSep)){
			String[] l = parseLine(line);
			
			if (l != null){
				//System.out.println("parsed line:"+ l[0]+" "+ l[1]);
				String left = l[0];
				if (isUser(left)){
					cUser = l[1];
					//System.out.println("Adding user"+ cUser);
					rti.addUserAgent(cUser);
				} else if (isDisallow(left)){
					//System.out.println("Adding disallow"+ cUser+ ":"+l[1]);
					rti.addDisallowedLink(cUser, l[1]);
				} else if (isDelay(left)){
					//System.out.println("Adding delay"+ cUser+ ":"+l[1]);
					int delay = 1;
					try {
						delay = Integer.parseInt(l[1]);
					} catch (NumberFormatException e){

					}
					rti.addCrawlDelay(cUser, delay);
				} else {
					continue;
				}
			}
		}
		
		return rti;
	}
	
	private static String[] parseLine(String line){
		String[] toReturn = line.split(":");
		if (toReturn.length != 2) return null;
		
		toReturn[0] = toReturn[0].trim();
		toReturn[1] = toReturn[1].trim();
		
		return toReturn;
	}
	
	private static boolean isUser(String input){
		return input.equalsIgnoreCase("User-agent");
	}
	
	private static boolean isDisallow(String input){
		return input.equalsIgnoreCase("Disallow");
	}
	
	private static boolean isDelay(String input){
		return input.equalsIgnoreCase("Crawl-delay");
	}
}
