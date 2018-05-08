package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.ClientResponse;
import edu.upenn.cis455.crawler.ClientWrapper;
import edu.upenn.cis455.crawler.DateConvertor;
import edu.upenn.cis455.crawler.XPathCrawlerInfo;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.*;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

public class CrawlerBolt implements IRichBolt{


    Fields myFields = new Fields("CrawlerBoltURL", "Content", "ContentType", "needStore", "lastAccessed", "HostName", "FromURL");

	OutputCollector collector;

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

	@Override
	public void cleanup() {
		// Do nothing

	}
	
	// run head to see if want to crawl
	private ClientResponse sendHeadReq(String curUrl, long lastAccessed) throws InterruptedException, IOException{
		
		XPathCrawlerInfo.checkTerminate();
		
		// send head request
		ClientWrapper cw = new ClientWrapper(curUrl, "HEAD");
//		System.out.println("Getting:"+curUrl);
		
		// add header if necessary
        if (lastAccessed != 0){
            String la = DateConvertor.longToStr(lastAccessed);
            cw.addReqHeader("If-Modified-Since",la);
        }
		
		// send
		ClientResponse cr = cw.process();
		XPathCrawlerInfo.increment();
		return cr;
	}
	
	// check response from head request
	// returns 1 for valid page, -1 for invalid, 0 for redirecting
	public int checkResponse(ClientResponse cr, String hostName, String fromUrl) throws InterruptedException{
		if (cr.status == null) return -1;
		int code = -1;
		try{
			code = Integer.parseInt(cr.status);
			//System.out.println("Status code is: "+ code);
		} catch (Exception e){
			System.out.println(e);
			return -1;
		}
		
		// response is error
		if (code >= 400) return -1;
		
		// already downloaded or moved
		switch (code){
			case 304:
				return 0;
			case 302:
			case 301:
				if (cr.header.containsKey("location") && cr.header.get("location") != null && cr.header.get("location").size() > 0 ){
					String redirect =cr.header.get("location").get(0);
					this.collector.emit(new Values<Object>("https://"+hostName+redirect, null, "redirect", "false", "", hostName, fromUrl));
				} 
				return -1;
			default:
				
		}
		
		// check size
        if (cr.header.containsKey("content-length") && Long.parseLong(cr.header.get("content-length").get(0)) > XPathCrawlerInfo.maxSize){
            System.out.println("ContentFile too big: " +Long.parseLong(cr.header.get("content-length").get(0))+ " "+ XPathCrawlerInfo.maxSize);
            return -1;
        }

        // check language
        if (cr.header.containsKey("content-language") && !validLanguage(cr.header.get("content-language").get(0))){
			return -1;
        }

		// check type
		if (!cr.header.containsKey("content-type")) return -1;
		else {
			String type = cr.header.get("content-type").get(0);
			if (type == null) return -1;
			else {
                return validType(type);
            }
		}
		
//		return -1;
	}

	private boolean validLanguage(String langs){
		String[] langAr = langs.split(";");
		for (String lang: langAr){
			if (lang.equalsIgnoreCase("en")  || lang.contains("en")){
				return true;
			}
		}
		System.out.println("Not supported language");
		return false;
	}

	// check if type is valid
	private int validType(String types){
//		System.out.println("ContentFile type is:" + types);
		String[] typeAr = types.split(";");
		for (String type: typeAr){
            type = type.trim();
//            System.out.println(type);
			if (type.equalsIgnoreCase("text/html")  || type.contains("text/html")){
				return 1;
			} else if (type.equalsIgnoreCase("application/pdf") || type.contains("application/pdf")){
                return 2;
            }
		}
		System.out.println("Wrong file type");
		return -1;
	}
	
	// download a page
	private ClientResponse downloadFile(String curUrl) throws IOException{
		ClientWrapper cw = new ClientWrapper(curUrl, "GET");
		ClientResponse cr = cw.process();
		return cr;
	}

	private String downloadPDF(String curUrl) {
		try {
			URL url = new URL(curUrl);
			InputStream in = url.openStream();
			File downloadedPDF = new File("pdf.jpg");
			FileOutputStream fos = new FileOutputStream(downloadedPDF);

			int length = -1;
			byte[] buffer = new byte[1024];// buffer for portion of data from connection
			while ((length = in.read(buffer)) > -1) {
				fos.write(buffer, 0, length);
			}
			fos.close();
			in.close();

			File file = new File("pdf.jpg");
			PDDocument document = PDDocument.load(file);

			//Instantiate PDFTextStripper class
			PDFTextStripper pdfStripper = new PDFTextStripper();

			//Retrieving text from PDF document
			String text = pdfStripper.getText(document);
//        System.out.println(text);

			//Closing the document
			document.close();
			return text;
		} catch (IOException e){
			return null;
		}
    }

	@Override
    public void execute(Tuple input) {
//        System.out.println("Exceuting Crawler Bolt: "+getExecutorId() + ": " + input.toString());
        String curUrl = input.getStringByField("SpoutURL");
        String laStr = input.getStringByField("LastAccessed");
		String hostName = input.getStringByField("HostName");
		String fromUrl = input.getStringByField("FromURL");
        long la = laStr.length() > 0 ? Long.parseLong(laStr) : 0;

//        System.out.println("CURRENT URL:" + curUrl+" "+la+ " "+ laStr);

        ClientResponse cr;
        try {
            // send head request
            cr = sendHeadReq(curUrl, la);
            if (cr == null) return;

            // check if need to get page
            int proceed = checkResponse(cr, hostName, fromUrl);
            long newDownloadTime = System.currentTimeMillis();
            switch (proceed){
                case 2:
                    System.out.println(curUrl+ " : Downloading PDF");
                    String downloadedPDF = downloadPDF(curUrl);
					if (downloadedPDF == null) return;
                    this.collector.emit(new Values<Object>(curUrl, downloadedPDF, "pdf", "true", newDownloadTime+"", hostName, fromUrl));
                    break;
                case 1:
                    System.out.println(curUrl+ " : Downloading");
                    ClientResponse downloadedFile = downloadFile(curUrl);
					if (downloadedFile == null) return;
                    this.collector.emit(new Values<Object>(curUrl, downloadedFile.content, "text/html", "true", newDownloadTime+"", hostName, fromUrl));

                    break;
                case 0:
                    System.out.println(curUrl+ " : Not modified");
                    this.collector.emit(new Values<Object>(curUrl, null, "local", "false", la + "", hostName, fromUrl));

                    break;
                case -1:
                    System.out.println(curUrl+ " : Not downloaded");
                    break;
                default:
                    break;
            }
        } catch (InterruptedException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
//            collector.emitEndOfStream();
        }
    }

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		// Do nothing
		this.collector = collector;
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
		this.collector.setRouter(router);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}
}
