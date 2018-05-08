package edu.upenn.cis455.database;

import java.util.List;

public class MySQLWrapper implements IndexingSQL{
	
    public boolean Initialize(){
        return true;
    }
    
    public boolean putRecord(String word, String url,
                             int tf,
                             float capitalPercent,
                             float titlePercent,
                             float linkPercent,
                             float emphasisPercent,
                             float metaPercent,
                             float headingScore,
                             float positionScore) throws IllegalArgumentException {
    	
    	
    	System.out.format("%s, %s, %d, %f, %f, %f, %f, %f, %f, %f\n", word, url, tf, capitalPercent, titlePercent,
    			linkPercent, emphasisPercent, metaPercent, headingScore, positionScore);
        return true;
    }
    
    public IndexingItem getRecord(String word, String url){
        return null;
    }
    
    public List<IndexingItem> getAllRecords(){
        return null;
    }
    public MySQLWrapper(){
        Initialize();
    }

}
