package ta.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;

import org.apache.commons.codec.binary.Base64;

import ta.logger.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by arielbaruch on 06/12/14.
 */
public class EC2Util {
    public static final String SIMPLE_LINUX = "ami-b66ed3de";
    private AmazonEC2 ec2;

    public EC2Util(AWSCredentials credentials) {
        this.ec2 = new AmazonEC2Client(credentials);
    }

    public EC2Util(String accessKey, String secretKey) {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey,secretKey);
        this.ec2 = new AmazonEC2Client(credentials);
    }


    public List<Instance> getInstancesList(){
        List<Instance> instances = new ArrayList<Instance>();

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        
        DescribeInstancesResult result = null;
        try{
        	result = this.ec2.describeInstances(request);
        }
        catch(Exception e){
        	Logger.INSTANCE.error("Error get instances list: "+e.getMessage());
        }
        
        if(result != null){
        	List<Reservation> reservations = result.getReservations();
            for(Reservation reservation : reservations){
                List<Instance> resInstances = reservation.getInstances();
                instances.addAll(resInstances);
            }
        }
        
        return instances;
    }

    public void attachTag(List<Instance> instances, Tag tag){
        try {
            for (Instance instance : instances) {
                CreateTagsRequest createTagsRequest = new CreateTagsRequest();
                createTagsRequest.withResources(instance.getInstanceId()) //
                        .withTags(tag);
                this.ec2.createTags(createTagsRequest);

            }
        }
        catch (AmazonClientException e){
            Logger.INSTANCE.error("[Error] attaching tag failed. " + e.getMessage());
        }
    }

    
    public List<Instance> createInstances(String imageId, Integer minCount, Integer maxCount,String user_data){
        try {
            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-b66ed3de)
            RunInstancesRequest request = new RunInstancesRequest(imageId,minCount,maxCount);
            request.setInstanceType(InstanceType.T2Small.toString());

            request.setUserData(user_data);
            request.withKeyName("ariel");
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            
            Logger.INSTANCE.info("Launch instances: " + instances);

            return instances;

        } catch (AmazonServiceException ase) {
            Logger.INSTANCE.error("Caught Exception: " +  ase.getMessage() );            
        }

        return null;
    }

    private boolean isTerminated(List<Instance> instances){
    	boolean ans = true;
    	for(Instance ins : instances){
    		if( !ins.getState().getName().equals("terminated") ){
    			ans = false;
    			break;
    		}
    	}
        return ans;
    }
    
    public Instance initManager(){
        boolean isExists = false;
        Instance manager = null;
        
        List<Instance> managers = findInstance("manager");
        
        if(!managers.isEmpty() && !isTerminated(managers) ){
        	manager = findWorkingManager(managers);
            isExists = true;
        }

        if(!isExists){
            List<Instance> instances = createInstances(SIMPLE_LINUX, 1, 1, createManagerUserData() );
            attachTag( instances,new Tag("name","manager") );

            manager = instances.get(0);
            Logger.INSTANCE.info("Created Manager Instance" );            

        }
        else{
            Logger.INSTANCE.info("Manager Instance Already Exists" );            
        }
        return manager;
    }

    private Instance findWorkingManager(List<Instance> managers) {
    	for(Instance ins : managers){
    		if( ins.getState().getName().equals("running") ){
    			return ins;
    		}
    	}
        return null;
    }

	public List<Instance> findInstance(String instanceName){
        List<Instance> results = new ArrayList<Instance>();

        List<Instance> instances = getInstancesList();
        for (Instance ins : instances){
            if( ins.getTags().contains( new Tag("name",instanceName) ) ){
                results.add(ins);
            }
        }

        return results;
    }


    public void terminateInstance(List<Instance> instance){
        List<String> ids = new ArrayList<String>();
        for(Instance ins : instance){
            ids.add(ins.getInstanceId());
        }
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
        terminateInstancesRequest.setInstanceIds(ids);
        try{
            ec2.terminateInstances(terminateInstancesRequest);
        }
        catch(Exception e){
        	Logger.INSTANCE.error("Failed terminating instances.");
        }
    }
    
    private boolean isTerminated(Instance ins){
        return ins.getState().getName().equals("terminated");
    }

    private String createManagerUserData(){
    	String base_name = "manager";
       	String jar_name = base_name+".jar";
    	String zip_name = base_name+".zip";
    	String s3_url = "https://s3.amazonaws.com/ariel-rotem-bgu/";
    	String password = "dsps2014";
    	
        String rawData = "#! /bin/bash\n" +
  				"curl -O -L "+s3_url+""+zip_name+"\n" +
  				"unzip -P "+password+" "+zip_name+"\n" +
  				"sleep 5 \n"+
                "chmod 777 "+jar_name+"\n" +
                "java -jar "+jar_name+"\n";

        String script = new String( Base64.encodeBase64(rawData.getBytes()) );

        return script;
    }

    
    public String createWorkerUserData(){
    	String name = "worker";
        String jar_name = name+".jar";
        String zip_name = name+".zip";
    	String password = "dsps2014";
    	String s3_url = "https://s3.amazonaws.com/ariel-rotem-bgu/";

        String stanford_corenlp = "stanford-corenlp-3.3.0.jar";
        String stanford_corenlp_models = "stanford-corenlp-3.3.0-models.jar";
        String ejml = "ejml-0.23.jar";
        String joliday = "jollyday-0.4.7.jar";

        String rawData = "#! /bin/bash\n" +
  				"curl -O -L "+s3_url+""+stanford_corenlp+"\n" +
  				"curl -O -L "+s3_url+""+stanford_corenlp_models+"\n" +
  				"curl -O -L "+s3_url+""+ejml+"\n" +
  				"curl -O -L "+s3_url+""+joliday+"\n" +  				
  				"curl -O -L "+s3_url+""+zip_name+"\n" +
  				"unzip -P "+password+" "+zip_name+"\n" +        
        
                "java -cp .:"+jar_name+":"+stanford_corenlp+":"+stanford_corenlp_models+":"+ejml+":"+joliday+" -jar "+jar_name+"\n";

        String script = new String( Base64.encodeBase64( rawData.getBytes() ) );

        return script;
    }

}
