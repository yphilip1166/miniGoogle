package edu.upenn.cis455.crawler;

import com.sleepycat.persist.model.Persistent;

/**
 * Created by Carlton on 4/30/18.
 */
@Persistent
public class URLWithData {
    private String url;
    private String lastAccessedTime;
    private String fromUrl;

    public URLWithData(){
        this.url = "";
        this.lastAccessedTime = "";
        this.fromUrl = "";
    }

    public URLWithData(String url){
        this.url =  url;
        this.lastAccessedTime = "";
        this.fromUrl = "";
    }

    public URLWithData(String url, String la, String fromUrl){
        this.url = url;
        this.lastAccessedTime = la;
        this.fromUrl = fromUrl;
    }

    public String getURL(){
        return this.url;
    }

    public String getLastAccessedTime(){
        return this.lastAccessedTime;
    }

    public String getFromUrl(){
        return this.fromUrl;
    }

    public int hashCode(){
       return url.hashCode();
    }

    public boolean equals(URLWithData other){
        return this.url.equals(other.getURL());
    }
}
