package ta.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ta.logger.Logger;


/**
 * Created by arielbaruch on 06/12/14.
 */
public class SQSUtil {
    public static final String ID = "sqsUtil";

    private AmazonSQS sqs;

    public SQSUtil(AWSCredentials credentials) {
        this.sqs = new AmazonSQSClient(credentials);
    }

    public String createQueue(String name){
        String myQueueUrl = null;
        System.out.println("Creating a new SQS queue called "+name+".\n");
        try{
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(name);
            Map<String,String> attributes = new HashMap<String, String>();
            attributes.put("ReceiveMessageWaitTimeSeconds","20");
            attributes.put("VisibilityTimeout","30");

            createQueueRequest.setAttributes(attributes);
            myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
        return myQueueUrl;
    }

    public void listQueues(){
        try{
            // List queues
            System.out.println("Listing all queues in your account.\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();

        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
    }

    public void sendMessage(String queueURL, String message){
        try{
            sqs.sendMessage(new SendMessageRequest(queueURL, message));

        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
    }
    
    public void sendMessage(String queueURL, String body, Map<String, MessageAttributeValue> attributes){
    	SendMessageRequest messageObj = new SendMessageRequest(queueURL, body);
    	messageObj.setMessageAttributes(attributes);
        try{
            sqs.sendMessage(messageObj);

        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
    }
    
    public List<Message> receiveMessages(String queueURL){
        List<Message> messages = null;
        try{
            System.out.println("Receiving messages from MyQueue.\n");
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL);
            receiveMessageRequest.setWaitTimeSeconds(20);
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
                System.out.println("  Message");
                System.out.println("    MessageId:     " + message.getMessageId());
                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());
                for (Map.Entry<String, String> entry : message.getAttributes().entrySet()) {
                    System.out.println("  Attribute");
                    System.out.println("    Name:  " + entry.getKey());
                    System.out.println("    Value: " + entry.getValue());
                }
            }
            System.out.println();

        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
        return messages;
    }

    public Message receiveNormalMessage(String queueURL){
    	return receiveMessage(queueURL, null);
    }
    
    public List<Message> receiveProcessedMessage(String queueURL){
    	List<String> attrsList = new ArrayList<String>();
        attrsList.add("task");
        attrsList.add("uuid");
        attrsList.add("entities");
        attrsList.add("sentiment");
        attrsList.add("outputFilename");
        
    	return receiveSomeMessages(queueURL, attrsList);
    }
    
    private List<Message> receiveSomeMessages(String queueURL, List<String> attrsList) {
        List<Message> messages = null;
        try{
            
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL);
            receiveMessageRequest.setMaxNumberOfMessages(10);
            receiveMessageRequest.setMessageAttributeNames(attrsList);

            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
        if(messages != null && messages.isEmpty()){
        	messages = null;
        }
        return messages;
	}

	public Message receiveMessage(String queueURL, List<String> attrsList){
        Message message = null;
        try{
            System.out.print(".");          
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL);
            receiveMessageRequest.setMaxNumberOfMessages(1);
            
            if(attrsList != null){

                receiveMessageRequest.setMessageAttributeNames(attrsList);
            }

            ReceiveMessageResult receivedReq = sqs.receiveMessage(receiveMessageRequest);
            if (receivedReq != null) {
                List<Message> messages = receivedReq.getMessages();
                if(messages != null && !messages.isEmpty() )
                    message = messages.get(0);
            }

        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
        return message;
    }

    public void deleteMessage(String queueURL, Message message){
        try{
            // Delete a message
            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(queueURL, messageReceiptHandle));

        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
    }

    public void deleteQueue(String queueURL){
        try{
            // Delete a queue
            System.out.println("Deleting the test queue.\n");
            sqs.deleteQueue(new DeleteQueueRequest(queueURL));

        }
        catch (AmazonServiceException ase) {
            handleAmazonServiceException(ase);
        }
        catch (AmazonClientException ace) {
            handleAmazonClientException(ace);
        }
    }

    private void handleAmazonServiceException(AmazonServiceException ase){
    	System.out.println(ase.getMessage());
        Logger.INSTANCE.error("Error  : " + ase.getMessage());            
    }

    private void handleAmazonClientException(AmazonClientException ace){
        Logger.INSTANCE.error("Error  : " + ace.getMessage());            

    }
}
