package ta.logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import ta.utils.BucketNames;
import ta.utils.QueueNames;
import ta.utils.S3Util;
import ta.utils.SQSUtil;

/**
 * Created by arielbaruch on 07/12/14.
 * Custom Logger 
 * Writes to console, file and AWS queue.
 */
public class Logger {
	public final static String INFO = "Info";
	public final static String ERROR = "Error";
	public final static String ALL = "All";
	
    public final static Logger INSTANCE = new Logger();
    private SQSUtil sqsUtil;
    private S3Util s3util;
    private String id;
    private StringBuilder logs;
    private String logFilename;
    
    //private constructor for Singleton purposes 
    private Logger() {
    }

    public void init(String id, SQSUtil sqsUtil, S3Util s3util){
        this.sqsUtil = sqsUtil;
        this.s3util = s3util;

        this.id = id;
        this.logs = new StringBuilder();
        
        DateTimeFormatter fmt = DateTimeFormat.forPattern("dd_MM_HH_mm");
        String formattedTime = fmt.print(new DateTime());

        this.logFilename = "log_"+id+"_"+formattedTime+".log";
    }

    public void info(String message){
            LogMessage logMessage = new LogMessage(id,INFO,message);
            String msg = logMessage.toString();
            System.out.println(msg);
            
            logs.append(msg);
            logs.append("\n");
    }
    
    public void error(String message){
        LogMessage logMessage = new LogMessage(id,ERROR,message);
        String msg = logMessage.toString();
        
        System.out.println(msg);
        
        logs.append(msg);
        logs.append("\n");
        
        sqsUtil.sendMessage(QueueNames.LOG_QUEUE_NAME,msg);
    }
    
    public void dumpToFile(){
	  Writer writer = null;
	  try {
	      writer = new FileWriter(new File(logFilename), true);
	      writer.append(this.logs.toString());
	  } 
	  catch (IOException ex) {
	  	System.out.println("Error writing log file: " + ex.getMessage());
	  }
	  finally{
	    	try {
				writer.close();
			} 
	    	catch (IOException e) {
				System.out.println("Failed closing log writer.");
			}
	  }
    }
    
    public void uploadLog(){
    	s3util.upload(BucketNames.LOGS_BUCKET_NAME, logFilename, new File(logFilename));
    }
 
}
