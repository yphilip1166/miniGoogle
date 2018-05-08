package edu.upenn.cis455.storage;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Created by Carlton on 4/23/18.
 */
@Entity
public class ContentFile {
    @PrimaryKey
    private String contentID = null;
    private String content = null;

    public ContentFile(){

    }

    public ContentFile(String contentID, String content){
        setContentID(contentID);
        setContent(content);
    }

    public void setContentID(String contentID){
        this.contentID = contentID;
    }

    public void setContent(String content){
        this.content = content;
    }

    public String getContentID(){
        return this.contentID;
    }

    public String getContent(){
        return this.content;
    }

}
