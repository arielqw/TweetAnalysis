import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import ta.logger.Logger;
import ta.utils.EC2Util;
import ta.utils.QueueNames;
import ta.utils.S3Util;
import ta.utils.SQSUtil;

/**
 * Created by arielbaruch on 05/12/14.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
    	
        AWSCredentials credentials = getCredentials();

        /* Initialize AWS Utilities */
        EC2Util ec2Util = new EC2Util(credentials);
        S3Util s3Util = new S3Util(credentials);
        SQSUtil sqsUtil = new SQSUtil(credentials);

        /* Initialize Logger */
        Logger log = Logger.INSTANCE;
        log.init("Manager",sqsUtil, s3Util);
        
        /* Initialize Manager */
        Manager manager = new Manager(ec2Util, s3Util, sqsUtil);

        manager.start();
        
    }


    private static AWSCredentials getCredentials() {
        AWSCredentials credentials = null;
        credentials = new BasicAWSCredentials("AKIAJCG5CV5E5XOACGDQ","2JckDMp0zzP6RYc8/+/wU75uHB5L5neUooS34snD");
        return credentials;
    }

}
