package edu.upenn.cis455.database;

import java.util.List;

public interface IndexingSQL {
	
    // Before Use, make sure to call Initialize once.
    public boolean Initialize();
    // When word, url is null or when other parameters are negative, throw IllegalArgumentException
    // return true when successful, return false if there are some database conflict or errors
    public boolean putRecord(String word, String url,
                             int tf,
                             float capitalPercent,
                             float titlePercent,
                             float linkPercent,
                             float emphasisPercent,
                             float metaPercent,
                             float headingScore,
                             float positionScore) throws IllegalArgumentException;
    // return null if it doesn't exist or error
    public IndexingItem getRecord(String word, String url);
    // return all records
    public List<IndexingItem> getAllRecords();
}
