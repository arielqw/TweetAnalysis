import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import ta.logger.Logger;
import ta.utils.EC2Util;
import ta.utils.S3Util;
import ta.utils.SQSUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Created by arielbaruch on 05/12/14.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        InetAddress IP = InetAddress.getLocalHost();
       
        AWSCredentials credentials = getCredentials();

        EC2Util ec2Util = new EC2Util(credentials);
        S3Util s3Util = new S3Util(credentials);
        SQSUtil sqsUtil = new SQSUtil(credentials);

        UUID uuid = UUID.randomUUID();
        String id = "Local_" + uuid.toString().substring(0,3);

        /* Init ta.logger.Logger */
        Logger log = Logger.INSTANCE;
        log.init(id,sqsUtil, s3Util);

        /* get user arguments */
        String inputFileName = "";
        String outputFileName = "";
        int n = 1;
        boolean terminate = false;

        if(args.length < 3){
            System.out.println("Error: missing arguments, given: "+args.length+" arguments.");
            return;
        }
        else{
            inputFileName = args[0];
            outputFileName = args[1];
            n = new Integer(args[2]);

            if(args.length > 3){
                if(args[3].equals("terminate")){
                    terminate = true;
                }
            }
        }


        Application app = new Application(ec2Util, s3Util, sqsUtil,uuid,id, inputFileName, outputFileName, n, terminate, IP);
        app.start();
        
    }



    private static AWSCredentials getCredentials() {
        AWSCredentials credentials = null;
        credentials = new BasicAWSCredentials("AKIAJCG5CV5E5XOACGDQ","2JckDMp0zzP6RYc8/+/wU75uHB5L5neUooS34snD");
        return credentials;
    }

}
