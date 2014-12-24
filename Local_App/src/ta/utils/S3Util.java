package ta.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.InputStream;

import ta.logger.Logger;


/**
 * Created by arielbaruch on 06/12/14.
 */
public class S3Util {
    public static final String ID = "s3Util";

    private AmazonS3 s3;
    private AWSCredentials credentials;

    public S3Util(AWSCredentials credentials) {
        this.credentials = credentials;
        this.s3 = new AmazonS3Client(credentials);
    }

    public String createBucket(String name) {
        String bucketName =
                (credentials.getAWSAccessKeyId() + "-" + name.replace('\\', '-').replace('/', '-').replace(':', '-')).toLowerCase();
        try {
            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);
        }
        catch (AmazonServiceException ase){
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace){
            handleAmazonClientException(ace);
        }

        return bucketName;
    }

    public void upload(String bucketName,String key,File file){
        try{
            Logger.INSTANCE.info("Uploading a new object to S3: " + key);            
            s3.putObject(new PutObjectRequest(bucketName, key, file));
        }
        catch (AmazonServiceException ase){
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace){
            handleAmazonClientException(ace);
        }
    }

    public InputStream download(String bucketName, String key){
        S3Object object=null;
        try{
            object = s3.getObject(new GetObjectRequest(bucketName, key));
        }
        catch (AmazonServiceException ase){
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace){
            handleAmazonClientException(ace);
        }
        return (InputStream) object.getObjectContent();

    }

    public void listObjects(String bucketName){
        try{
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucketName));
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();
        }
        catch (AmazonServiceException ase){
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace){
            handleAmazonClientException(ace);
        }
    }

    public void deleteFile(String bucketName, String key){
        try{
            System.out.println("Deleting an object\n");
            s3.deleteObject(bucketName, key);
        }
        catch (AmazonServiceException ase){
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace){
            handleAmazonClientException(ace);
        }
    }

    public void deleteBucket(String bucketName){
        try{
            System.out.println("Deleting bucket " + bucketName + "\n");
            s3.deleteBucket(bucketName);
        }
        catch (AmazonServiceException ase){
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace){
            handleAmazonClientException(ace);
        }
    }


    private void handleAmazonServiceException(AmazonServiceException ase){        
        Logger.INSTANCE.error("Error  : " + ase.getMessage());            

    }

    private void handleAmazonClientException(AmazonClientException ace){
        Logger.INSTANCE.error("Error  : " + ace.getMessage());            
    }


}


