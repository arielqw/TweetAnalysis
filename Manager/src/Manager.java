import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import ta.logger.Logger;
import ta.utils.Base64Serializer;
import ta.utils.BucketNames;
import ta.utils.EC2Util;
import ta.utils.QueueNames;
import ta.utils.S3Util;
import ta.utils.SQSUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

/**
 * Created by arielbaruch on 07/12/14.
 */
public class Manager {
    private final int MAX_MESSAGES_IN_RAM = 10000;
    private final int INSTANCES_LIMIT = 20;
    
    private EC2Util ec2Util;
    private S3Util s3Util;
    private SQSUtil sqsUtil;

    private boolean isTakingNewTasks;
    private Map<String,TaskStatus> tasksStatus;
    
    private List<Instance> workers;
	private int workersSize;
	
	private Map<String,StringBuilder> files_container;
	private int ram_message_counter;
	
    public Manager(EC2Util ec2Util, S3Util s3Util, SQSUtil sqsUtil) {
        this.ec2Util = ec2Util;
        this.s3Util = s3Util;
        this.sqsUtil = sqsUtil;
        this.tasksStatus = new HashMap<String, TaskStatus>();
        this.isTakingNewTasks = true;
        this.workers = new ArrayList<Instance>();
        this.workersSize = 0;
        this.files_container = new HashMap<String, StringBuilder>();
        this.ram_message_counter = 0;
    }



    public void start() {
    	Logger.INSTANCE.info("Session Started");
    	//continue as long as did not receive 'terminate' message and have work to do
        while( isTakingNewTasks || !tasksStatus.isEmpty() ){
        	// 1. Try taking task
            if(isTakingNewTasks){
            	tryTakingTask();
            }
            // 2. Try taking proccessed tweets
            tryTakingProccessedTweets();
        }
        
        // 3. Terminate workers instances
        killWorkers();
        
        // 4. Terminate self
        sucide();
        
        // 5. upload log to S3
        Logger.INSTANCE.dumpToFile();
        Logger.INSTANCE.uploadLog();
    }

    private void tryTakingProccessedTweets() {
    	  List<Message> proccessedMessages = sqsUtil.receiveProcessedMessage(QueueNames.PROCESSED_TWEETS_QUEUE_NAME);
          if(proccessedMessages != null){
          	for(Message proccessed : proccessedMessages){
                  if(proccessed != null){
                	  
                	  //handle processed message
                      String currentUuid = handleProcessedTweet(proccessed);

                      //if exceeded memory limit - flush to flies
                      if(ram_message_counter == MAX_MESSAGES_IN_RAM){
                      	flushTweetsToFiles();
                      }
                      sqsUtil.deleteMessage(QueueNames.PROCESSED_TWEETS_QUEUE_NAME,proccessed);

                      //check if finished a task
                      if(this.tasksStatus.get(currentUuid).isDone()){
                          Logger.INSTANCE.info("Finished task for uid: "+currentUuid+" took "+this.tasksStatus.get(currentUuid).getDuration()+" ms");

                      	  flushTweetsToFiles();
                          //upload files
                          s3Util.upload(BucketNames.OUTPUT_BUCKET_NAME, currentUuid + ".entities",new File(currentUuid + ".entities"));
                          s3Util.upload(BucketNames.OUTPUT_BUCKET_NAME, currentUuid + ".sentiment",new File(currentUuid + ".sentiment"));
                          Logger.INSTANCE.info("Uploaded outputs to S3 ("+currentUuid+")");

                          //send message to done queue
                          sqsUtil.sendMessage(QueueNames.FINISHED_TASKS_QUEUE_NAME,currentUuid);
                          //remove from map
                          tasksStatus.remove(currentUuid);
                          Logger.INSTANCE.info("Remaining: "+tasksStatus.size()+" tasks");

                      }
                      
                  }            	
              }
          }		
	}



	private void tryTakingTask() {
        Message task_msg = sqsUtil.receiveNormalMessage(QueueNames.TASKS_QUEUE_NAME);
        if(task_msg != null){
        	Logger.INSTANCE.info("Got a new task");
            handleTask(task_msg);
            sqsUtil.deleteMessage(QueueNames.TASKS_QUEUE_NAME,task_msg);
        }		
	}



	public void sucide() {
    	Logger.INSTANCE.info("dieing now..");
		ec2Util.terminateInstance(ec2Util.findInstance("manager"));		

	}

	private void killWorkers() {
        ec2Util.terminateInstance(this.workers);
        this.workersSize = 0;
    }

	private String handleProcessedTweet(Message processed_tweet_msg) {
    	Map<String, MessageAttributeValue> attrs = processed_tweet_msg.getMessageAttributes();
    	String filename = attrs.get("outputFilename").getStringValue();
    	String task = attrs.get("task").getStringValue();
    	String uuid = attrs.get("uuid").getStringValue();

		//create a file container if not exists
        if(files_container.get(filename) == null){
        	files_container.put(filename, new StringBuilder());
        }
        
        StringBuilder sb = files_container.get(filename);
        
        if(task.equals("entities")){
        	sb.append("<"+ attrs.get("entities").getStringValue()+">");

            this.tasksStatus.get(uuid).incrementEntitiesCount();
        }
        else if( task.equals("sentiment") ){
        	sb.append("<"+ attrs.get("sentiment").getStringValue()+">");

            this.tasksStatus.get(uuid).incrementSentimentCount();

        }
        sb.append(processed_tweet_msg.getBody());
        sb.append("\n");
        
        ram_message_counter++;
        
        return uuid;
    }
   
    private void flushTweetsToFiles() {    	
        Logger.INSTANCE.info("flushing processed tweets to file");

    	for (Map.Entry<String, StringBuilder> entry : this.files_container.entrySet()) {
    	    String filename = entry.getKey();
    	    StringBuilder sb = entry.getValue();
    	    
    	    Writer writer = null;
            
            try {
                writer = new FileWriter(new File( filename ), true);
                writer.append(sb.toString());

            } catch (IOException ex) {
                Logger.INSTANCE.error("Failed writing to output file: "+ex.getMessage());
            }
        	finally {
                try {
                	writer.close();
                } catch (Exception ex) {
                    Logger.INSTANCE.error("Failed closing output file: "+ex.getMessage());
                }
            } 	    
    	}
    	
        //reset values
    	this.files_container = new HashMap<String, StringBuilder>();
    	this.ram_message_counter = 0;
    }

    private void handleTask(Message message) {
        TaskMessage task = (TaskMessage)Base64Serializer.deSerialize(message.getBody());
        if( task.isTerminate() ){
            this.isTakingNewTasks = false;
            Logger.INSTANCE.info("Received terminate Request");
        }
        else{
            InputStream stream = s3Util.download(BucketNames.INPUT_BUCKET_NAME,task.getUuid()+".input");
            Logger.INSTANCE.info("downloaded '"+task.getUuid()+".input'");

            int numOfTweets = addTweetsToQueue(stream,task.getUuid());

            summonWorkers(numOfTweets,task.getN());
            this.tasksStatus.put(task.getUuid(), new TaskStatus(numOfTweets,task.getUuid()));
        }
    }

    private void summonWorkers(int tweetsSize,int n) {
        Logger.INSTANCE.info("Requested "+n+" tweets per worker");

        int desiredWorkersSize = Math.min((tweetsSize / n), INSTANCES_LIMIT-1);

        Logger.INSTANCE.info("Need to summon "+desiredWorkersSize+" workers");

        desiredWorkersSize = desiredWorkersSize - this.workersSize;
        Logger.INSTANCE.info("Got "+this.workersSize+" running workers");

        if(desiredWorkersSize > 0){
            Logger.INSTANCE.info("Summoning "+desiredWorkersSize+" workers");

        	this.workersSize += desiredWorkersSize;
        	List<Instance> workers = ec2Util.createInstances(EC2Util.SIMPLE_LINUX,desiredWorkersSize,desiredWorkersSize,ec2Util.createWorkerUserData());
        	ec2Util.attachTag(workers,new Tag("name","worker"));
            this.workers.addAll( workers );
        }
    }

    private int addTweetsToQueue(InputStream stream, String uuid) {
    	long start_time = new DateTime().getMillis();
    	
        Logger.INSTANCE.info("Adding raw tweets to queue");
        
        int numOfTweets = 0;
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line = null;
        try {
            boolean keepReading = true;
            
        	Map<String, MessageAttributeValue> attrs = new HashMap<String, MessageAttributeValue>();
        	attrs.put("uuid", new MessageAttributeValue().withDataType("String").withStringValue(uuid) );
        	
            while ( keepReading ) {
                line = reader.readLine();

                if (line == null) { //end of file
                    keepReading = false;
                }
                else {
                	String task = "sentiment";
                	
                	attrs.put("task", new MessageAttributeValue().withDataType("String").withStringValue(task) );
                	attrs.put("outputFilename", new MessageAttributeValue().withDataType("String").withStringValue(uuid + "." + task) );
                	
                	
                	task = "entities";
                    sqsUtil.sendMessage(QueueNames.RAW_TWEETS_QUEUE_NAME, line, attrs);
                    
                	attrs.put("task", new MessageAttributeValue().withDataType("String").withStringValue(task) );
                	attrs.put("outputFilename", new MessageAttributeValue().withDataType("String").withStringValue(uuid + "." + task) );

                    sqsUtil.sendMessage(QueueNames.RAW_TWEETS_QUEUE_NAME, line, attrs);
                    numOfTweets++;
                }
            }

        } catch (IOException e) {
            Logger.INSTANCE.error("Failed reading from input file: "+e.getMessage());

        } finally {
            try {
            	reader.close();
            } catch (Exception ex) {
                Logger.INSTANCE.error("Failed closing input file: "+ex.getMessage());
            }
        }
        long end_time = new DateTime().getMillis();
        Logger.INSTANCE.info("Finished adding "+numOfTweets+"raw tweetes to queue. took: "+(end_time-start_time)+" ms");

        return numOfTweets;
    }


}
