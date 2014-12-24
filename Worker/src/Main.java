import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import ta.utils.S3Util;
import ta.utils.SQSUtil;

/**
 * Created by arielbaruch on 05/12/14.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {

        AWSCredentials credentials = getCredentials();

        /* Init ta.utils */
        SQSUtil sqsUtil = new SQSUtil(credentials);

        /* Init Worker */
        Worker worker = new Worker(sqsUtil);
        System.out.println("[Info] Worker Started");
        worker.start();

    }

    private static AWSCredentials getCredentials() {
        AWSCredentials credentials = null;
        credentials = new BasicAWSCredentials("AKIAJCG5CV5E5XOACGDQ","2JckDMp0zzP6RYc8/+/wU75uHB5L5neUooS34snD");
        return credentials;
    }


}
