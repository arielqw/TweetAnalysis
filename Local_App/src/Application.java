import com.amazonaws.services.sqs.model.Message;

import ta.logger.Logger;
import ta.utils.Base64Serializer;
import ta.utils.BucketNames;
import ta.utils.EC2Util;
import ta.utils.QueueNames;
import ta.utils.S3Util;
import ta.utils.SQSUtil;

import java.io.*;
import java.net.InetAddress;
import java.util.UUID;

import org.joda.time.DateTime;

/**
 * Created by arielbaruch on 07/12/14.
 */
public class Application {
    public final String ID;

    private final UUID uuid;
    private EC2Util ec2Util;
    private S3Util s3Util;
    private SQSUtil sqsUtil;
    private String inputFileName;
    private String outputFileName;
    private int n;
    private boolean terminate;
    private InetAddress IP;

    public Application(EC2Util ec2Util, S3Util s3Util, SQSUtil sqsUtil,UUID uuid,String id, String inputFileName, String outputFileName, int n, boolean terminate, InetAddress IP) {
        this.uuid = uuid;
        this.ID = id;
        this.ec2Util = ec2Util;
        this.s3Util = s3Util;
        this.sqsUtil = sqsUtil;
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
        this.n = n;
        this.terminate = terminate;
        this.IP = IP;

    }


    public void start(){
    	long start_time = new DateTime().getMillis();
    	
    	Logger.INSTANCE.info("Session Started");
        boolean shouldTerminateManager = terminate;

        Logger.INSTANCE.info("Client started ip: " + IP );
        Logger.INSTANCE.info("user params: " + "input filename: "+inputFileName+" output filename: "+outputFileName+" n: "+n+" terminate: "+terminate );

        //upload input file to S3
        String UniqueInputFileName = this.uuid.toString()+".input";
        s3Util.upload(BucketNames.INPUT_BUCKET_NAME,UniqueInputFileName,new File(inputFileName));
        Logger.INSTANCE.info("Uploaded "+UniqueInputFileName+" to "+BucketNames.INPUT_BUCKET_NAME);

        sqsUtil.sendMessage(QueueNames.TASKS_QUEUE_NAME, Base64Serializer.serialize(new TaskMessage(uuid.toString(),inputFileName,outputFileName,n,false)));
        Logger.INSTANCE.info("Added task to tasks queue.");

        ec2Util.initManager();

        waitForTaskToFinish();

        if(shouldTerminateManager){
            //send termination request
            sqsUtil.sendMessage(QueueNames.TASKS_QUEUE_NAME, Base64Serializer.serialize(new TaskMessage("NA","NA","NA",0,true)));
        }

        //download files
        InputStream sentimentStream = s3Util.download(BucketNames.OUTPUT_BUCKET_NAME,uuid+".sentiment");
        InputStream entitiesStream = s3Util.download(BucketNames.OUTPUT_BUCKET_NAME,uuid+".entities");
        
        writeSentimentHTML(sentimentStream);
        writeEntitiesHTML(entitiesStream);

    	long end_time = new DateTime().getMillis();

        Logger.INSTANCE.info("Local Application terminated. id: "+this.ID+" took "+(end_time-start_time)+" ms");
        
        Logger.INSTANCE.dumpToFile();
        Logger.INSTANCE.uploadLog();
    }

    private void writeEntitiesHTML(InputStream entitiesStream) {
        Writer writer = null;
        BufferedReader reader = null;

        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(this.outputFileName+"1.html"), "utf-8"));
            reader = new BufferedReader(new InputStreamReader(entitiesStream));

            String inputLine = null;
            try {
                boolean keepReading = true;

                writer.append("<html>\n" +
                        "<body>");

                while ( keepReading ) {
                    inputLine = reader.readLine();

                    if(inputLine == null ){
                        keepReading = false;
                    }
                    else {
                        String entities = getEntities(inputLine); //<Entities> tweet-message => <span style='color:yellow'>tweet-msg</span> entities
                        String tweet = getTweet(inputLine); //<Entities> tweet-message => <span style='color:yellow'>tweet-msg</span> entities
                        writer.append("<p> <b><font>"+tweet+ " " + entities + "</font></b></p>\n");


                    }
                }

                writer.append("\t</body>\n" +
                        "</html>");

            } catch (IOException e) {
                Logger.INSTANCE.error("Failed reading line from file: "+e.getMessage());
            } finally {
                try {reader.close();} catch (Exception ex) {  Logger.INSTANCE.error("Error closing file: "+ex.getMessage()); }
            }


        } catch (IOException ex) {
            // report
        } finally {
            try {writer.close();} catch (Exception ex) {}
        }
    }

    private String getTweet(String inputLine) {
        return inputLine.substring(inputLine.indexOf('>')+1,inputLine.length());
    }

    private String getEntities(String inputLine) {
        if(inputLine.indexOf('>') <= 1){
            return "";
        }
        else{
            return inputLine.substring(1,inputLine.indexOf('>'));
        }
    }

    private void writeSentimentHTML(InputStream sentimentStream) {
        Writer writer = null;
        BufferedReader reader = null;

        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(this.outputFileName+"2.html"), "utf-8"));
            reader = new BufferedReader(new InputStreamReader(sentimentStream));

            String inputLine = null;
            try {
                boolean keepReading = true;

                writer.append("<html>\n" +
                        "<body>\n");

                while ( keepReading ) {
                    inputLine = reader.readLine();
                    if(inputLine == null ){
                        keepReading = false;
                    }
                    else{
                        String sentiment= getSentiment(inputLine); 
                        String color ="";
                        switch (sentiment){
                            case "-3":
                                color = "DarkRed";
                                break;

                            case "-2":
                                color = "Red";
                                break;

                            case "-1":
                                color = "OrangeRed";
                                break;

                            case "0":
                                color = "Black";
                                break;

                            case "1":
                                color = "LightGreen";
                                break;

                            case "2":
                                color = "Green";
                                break;

                            case "3":
                                color = "DarkGreen";
                                break;
                        }
                        String tweet= getTweet(inputLine);
                        writer.append("<p> <b><font color=\""+color+"\">"+tweet+"</font></b></p>\n");
                    }
                }

                writer.append("</body>\n" +
                        "</html>");

            } catch (IOException e) {
                Logger.INSTANCE.error("Failed writing to file: "+e.getMessage());
            } finally {
                try {reader.close();} catch (Exception ex) { Logger.INSTANCE.error("Error closing file: "+ex.getMessage()); }
            }


        } catch (IOException ex) {
        	Logger.INSTANCE.error("IO Exception: "+ex.getMessage());
        } finally {
            try {writer.close();} catch (Exception ex) { Logger.INSTANCE.error("Error closing file: "+ex.getMessage()); }
        }
    }

    private String getSentiment(String inputLine) {
        if(inputLine.indexOf('>') <= 1){
            return "";
        }
        else{
            return inputLine.substring(1,inputLine.indexOf('>'));
        }
    }

    private void waitForTaskToFinish() {
        boolean isTaskFinished = false;
        while(!isTaskFinished){
            Message message = sqsUtil.receiveNormalMessage(QueueNames.FINISHED_TASKS_QUEUE_NAME);
            if(message != null){
                if(message.getBody().equals(uuid.toString())){
                    sqsUtil.deleteMessage(QueueNames.FINISHED_TASKS_QUEUE_NAME,message);
                    isTaskFinished = true;
                }
            }
        }
        Logger.INSTANCE.info("Found that task has finished.");
    }


	public String getID() {
		return ID;
	}
}
