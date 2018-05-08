
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import com.amazonaws.services.s3.model.*;
import org.apache.commons.codec.digest.DigestUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.lang.System;
/**
 * Download the webpage and store to the database
 * @author dongtianxiang
 *
 */
public class DownloadFileS3 {

    public static void uploadfileS3(AWSCredentials credentials, String url, byte[] content) {
        System.out.println("Creating S3 Client");
        AmazonS3 s3client = new AmazonS3Client(credentials);
        String bucketName   = "quantumfilesystem";

        try {
            ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(content);
            ObjectMetadata md = new ObjectMetadata();
            md.setContentLength(content.length);

            String keyName = DigestUtils.sha1Hex(url);
            s3client.putObject(new PutObjectRequest(bucketName, keyName, contentsAsStream, md));
        } catch (AmazonServiceException ase) {
            System.err.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.err.println("Error Message:    " + ase.getMessage());
            System.err.println("HTTP Status Code: " + ase.getStatusCode());
            System.err.println("AWS Error Code:   " + ase.getErrorCode());
            System.err.println("Error Type:       " + ase.getErrorType());
            System.err.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.err.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.err.println("Error Message: " + ace.getMessage());
        }
    }

    /**
     * The API to get files from S3 in InputStream type. Please remember to close it when it's done
     * @param credentials  Related S3 credential information, can be read in ./conf/config.properties
     * @param folder   The requested folder from S3
     * @return The InputStream for the requested File. Null if the request failed.
     */
    public static InputStream downloadfileS3(AWSCredentials credentials, String folder){
        try {
            System.out.println("Creating S3 Client...");
            AmazonS3 s3client = new AmazonS3Client(credentials);
            System.out.println("Getting ListObject");
            ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName("cis555-database")
                    .withPrefix(folder);
            System.out.println("Listing Objects");
            ObjectListing objectListing = s3client.listObjects(listObjectRequest);
            List<S3ObjectSummary> s3ObjectSummaries = objectListing.getObjectSummaries();
            while (objectListing.isTruncated()) {
                System.out.println("While is Truncted");
                objectListing = s3client.listNextBatchOfObjects(objectListing);
                s3ObjectSummaries.addAll(objectListing.getObjectSummaries());
                break;
            }
            System.out.println("Begin listing Objects");
            int i = 1;
            for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
                String keyName = s3ObjectSummary.getKey();

                S3Object object = s3client.getObject(new GetObjectRequest("cis555-database", keyName));
                return object.getObjectContent();
            }
            return null;
        } catch (AmazonServiceException ase) {
            System.err.println("folder: "+folder);
            System.err.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.err.println("Error Message:    " + ase.getMessage());
            System.err.println("HTTP Status Code: " + ase.getStatusCode());
            System.err.println("AWS Error Code:   " + ase.getErrorCode());
            System.err.println("Error Type:       " + ase.getErrorType());
            System.err.println("Request ID:       " + ase.getRequestId());
            return null;
        } catch (AmazonClientException ace) {
            System.err.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.err.println("Error Message: " + ace.getMessage());
            return null;
        }
    }


    public static void main(String[] args) throws IOException {
        System.setProperty("KEY", "pbt6uJZ5dqBJG97GP/V54PPPmjSUq46HZtCURbYC");
        System.setProperty("ID","AKIAJWLJ2TM4HHDYPTMQ");

        AWSCredentials credentials = new BasicAWSCredentials(
                System.getProperty("ID"),
                System.getProperty("KEY"));

        InputStream input = downloadfileS3(credentials, "crawledpage");
        InputStreamReader streamReader = new InputStreamReader(input);
        BufferedReader bufferedReader = new BufferedReader(streamReader);
        StringBuilder sb = new StringBuilder();
        String line = null;
        while( (line = bufferedReader.readLine()) != null ) {
            sb.append(line + "\n");
        }
        input.close();
        System.out.println(sb.toString());
    }

    public void whatever() {

    }


}