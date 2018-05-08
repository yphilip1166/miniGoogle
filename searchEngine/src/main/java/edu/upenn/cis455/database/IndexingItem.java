package edu.upenn.cis455.database;

public class IndexingItem {
    String word;
    String url;
    public int tf;
    public float capitalPercent;
    public float titlePercent;
    public float linkPercent;
    public float emphasisPercent;
    public float metaPercent;
    public float headingScore;
    public float positionScore;

    public IndexingItem(){
        word = "";
        url = "";
        tf = 0;
        capitalPercent = 0.0f;
        titlePercent = 0.0f;
        linkPercent = 0.0f;
        emphasisPercent = 0.0f;
        metaPercent = 0.0f;
        headingScore = 0.0f;
        positionScore = 0.0f;
        IndexingSQL sql = getOne();
    }

    IndexingSQL getOne(){
        return null;
    }

    public IndexingItem(String word, String url,
                        int tf,
                        float capitalPercent,
                        float titlePercent,
                        float linkPercent,
                        float emphasisPercent,
                        float metaPercent,
                        float headingScore,
                        float positionScore){
        this.tf = tf;
        this.capitalPercent = capitalPercent;
        this.titlePercent = titlePercent;
        this.linkPercent = linkPercent;
        this.emphasisPercent = emphasisPercent;
        this.metaPercent = metaPercent;
        this.headingScore = headingScore;
        this.positionScore = positionScore;
    }
}

