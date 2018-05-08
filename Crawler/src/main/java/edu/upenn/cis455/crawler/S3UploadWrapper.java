package edu.upenn.cis455.crawler;

public class S3UploadWrapper{
    private String curUrl;
    private String fromUrl;
    private String content;

    public S3UploadWrapper(String curUrl, String fromUrl, String content){
        this.curUrl = curUrl;
        this.fromUrl = fromUrl;
        this.content = content;
    }

    public String getFromUrl(){return fromUrl;}

    public String getContent(){return content;}

    public String getCurUrl(){return curUrl;}


}
