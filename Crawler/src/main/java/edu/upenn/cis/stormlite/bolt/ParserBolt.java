package edu.upenn.cis.stormlite.bolt;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.S3UploadWrapper;
import edu.upenn.cis455.mapreduce.worker.PageRankUpdator;
import edu.upenn.cis455.mapreduce.worker.WorkerServerInfo;
import edu.upenn.cis455.storage.ContentFile;
import edu.upenn.cis455.storage.DBWrapper;
import edu.upenn.cis455.storage.Page;
import edu.upenn.cis455.storage.SHAEncryption;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;



public class ParserBolt implements IRichBolt{
	final String BUCKETNAME = "cis555-database";

	static Logger log = Logger.getLogger(PrintBolt.class);

	private List<S3UploadWrapper> s3Cache = Collections.synchronizedList(new ArrayList<S3UploadWrapper>());
	
	Fields myFields = new Fields("cururl", "lastAccessed", "HostName", "FromURL");
	OutputCollector collector;
    /**
     * To make it easier to debug: we have a unique ID for each
     * nstance of the PrintBolt, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

	@Override
	public void cleanup() {
		// Do nothing

	}

	@Override
	public void execute(Tuple input) {
//		System.out.println(getExecutorId() + ": " + input.toString());

		// get info from last bolt
//		System.out.println("Executing parser bolt");
		HashSet<String> urlSet = new HashSet<String>();
        HashMap<String, String> laMap = new HashMap<>();

		String curUrl = (String)input.getObjectByField("CrawlerBoltURL");
		String content = (String)input.getObjectByField("Content");
		String fileType = (String)input.getObjectByField("ContentType");
		String fromUrl = input.getStringByField("FromURL");
//		System.out.println(fileType);
		if (fileType.equals("local")){
			Page localFile = DBWrapper.getPage(curUrl);
			content = DBWrapper.getFile(localFile.getContentID()).getContent();
			fileType = localFile.getContentType();
		}
		String needStore = (String)input.getObjectByField("needStore");
		String la = (String)input.getObjectByField("lastAccessed");
		Document doc = content == null? null : Jsoup.parse(content, curUrl);

		// match channel xpath with content
		try {


			// extract links from file
			switch (fileType) {
				case "text/html":
					// simple text/html files
//					URL temp;
//					temp = new URL(curUrl);
//					String curFullUrl = temp.getProtocol()+"://"+temp.getHost() + temp.getPath();
//					curFullUrl = curFullUrl.substring(0, curFullUrl.lastIndexOf('/'));

					if (needStore.equals("true")){
						long newFileTime = System.currentTimeMillis();
						try {
							newFileTime = Long.parseLong(la);
						} catch (NumberFormatException e){

						}
						String contentID = SHAEncryption.encode(content);
                        if (!DBWrapper.containsFile(contentID)) {
                            ContentFile cf = new ContentFile(contentID, content);
                            DBWrapper.putFile(cf);
                            S3UploadWrapper wrapper = new S3UploadWrapper(curUrl, fromUrl, content);
							uploadToS3(wrapper);
                        }
						Page newContent = new Page(curUrl, contentID, fileType, newFileTime);
//						System.out.println("Dep " + fromUrl+","+curUrl);
						PageRankUpdator.addDependency(fromUrl, curUrl);
						WorkerServerInfo.incrementKeysRead();
						DBWrapper.putPage(newContent);
					}
//					System.out.println("FULL URL:" + curFullUrl);
					for (Element e: doc.select("a[href")) {
						String u = e.attr("abs:href");
						if(u==null){
							continue;
						}
						u = u.trim();
						if(u.length() == 0){
							continue;
						}
						//System.out.println("u:" + u );
						String toAdd = u;
						String fileLA = "";
//						if (u.startsWith("http") || u.startsWith("HTTP")){

//						} else {
//							toAdd = curFullUrl.endsWith("/")? curFullUrl+u:curFullUrl+"/"+u;
//						}

						if (DBWrapper.containsPage(toAdd)){
//							System.out.println(">>>>>Found ContentFile On disk");
							Page prvFile = DBWrapper.getPage(toAdd);
							fileLA = prvFile.getLastAccessed()+"";
//							System.out.println("ContentFile last accessed at:" + fileLA);
						}
						//PageRankUpdator.addDependency(curUrl, toAdd);
						urlSet.add(toAdd);
                        laMap.put(toAdd, fileLA);
					}
					break;
				case "pdf":
					if (needStore.equals("true")){
						long newFileTime = System.currentTimeMillis();
						try {
							newFileTime = Long.parseLong(la);
						} catch (NumberFormatException e){

						}
						String contentID = SHAEncryption.encode(content);
						if (!DBWrapper.containsFile(contentID)) {
							ContentFile cf = new ContentFile(contentID, content);
							DBWrapper.putFile(cf);
							S3UploadWrapper wrapper = new S3UploadWrapper(curUrl, fromUrl, content);
							uploadToS3(wrapper);
						}
//						System.out.println(curUrl);
						Page newContent = new Page(curUrl, contentID, fileType, newFileTime);
						DBWrapper.putPage(newContent);

						PageRankUpdator.addDependency(fromUrl, curUrl);
						WorkerServerInfo.incrementKeysRead();
					}
					break;
				case "redirect":
					// redirect urls
					urlSet.add(curUrl);
					break;
				default:
//					if (needStore.equals("true")){
//						temp = new URL(curUrl);
//						curFullUrl = temp.getProtocol()+"://"+temp.getHost() + temp.getPath();
//						curFullUrl = curFullUrl.substring(0, curFullUrl.lastIndexOf('/'));
//                        String contentID = SHAEncryption.encode(content);
//                        if (!DBWrapper.containsFile(contentID)) {
//                            ContentFile cf = new ContentFile(contentID, content);
//                            DBWrapper.putFile(cf);
//                        } else {
//                            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>Content Seen!!!!!!!");
//                        }
//						Page newContent = new Page(curUrl, contentID, fileType, System.currentTimeMillis());
//						DBWrapper.putPage(newContent);
//					}
					break;
			}

            HashSet<String> toReturn = new HashSet<>();
            for (String tempUrl: urlSet){
                String time = "";
                if (laMap.containsKey(tempUrl)){
                    time = laMap.get(tempUrl);
                }
//                toReturn.add();

                URL tURL = null;
                try{
					tURL = new URL(tempUrl);
					String host = tURL.getHost();
					this.collector.emit(new Values<Object>(tempUrl, time, host, curUrl));
				} catch (MalformedURLException e){
					System.out.println("base: "+curUrl);
					System.out.println("malformed: " +tempUrl);
				}
            }
//            System.out.println("In parser:" + toReturn);
//			this.collector.emit(new Values<Object>(toReturn));
		}
//		catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
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

	private synchronized void uploadToS3(S3UploadWrapper wrapper){
		if(s3Cache.size() > 10){
			AmazonS3 s3client = WorkerServerInfo.getAWSClient();
			try{
				for(S3UploadWrapper w: s3Cache){
					String url = w.getCurUrl();
					String fromUrl = w.getFromUrl();
					String content = w.getContent();
					
					String file = url + ","  + fromUrl + "\n" + content;
					String keyName = DigestUtils.sha1Hex(url);
					byte[] contentArray = file.getBytes(Charset.forName("UTF-8"));
					ByteArrayInputStream contentStream = new ByteArrayInputStream(contentArray);
					ObjectMetadata md = new ObjectMetadata();
					md.setContentLength(file.length());
					s3client.putObject(new PutObjectRequest(BUCKETNAME, "combined/crawledpage/"+keyName, contentStream, md));
				}
				s3Cache.clear();	
			}catch (AmazonServiceException ase){
				s3Cache.clear();
				System.out.println("Caught an AmazonServiceException, which " +
						"means your request made it " +
						"to Amazon S3, but was rejected with an error response" +
						" for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
			}catch (AmazonClientException ace) {
				s3Cache.clear();
				System.out.println("Caught an AmazonClientException, which " +
						"means the client encountered " +
						"an internal error while trying to " +
						"communicate with S3, " +
						"such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
			}
		} else {
			s3Cache.add(wrapper);
		}				
	}
}
