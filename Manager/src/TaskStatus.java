import org.joda.time.DateTime;

/**
 * Created by arielbaruch on 10/12/14.
 */
public class TaskStatus {
    private int totalEntitiesTweets;
    private int totalSentimentTweets;

    private int currentEntitiesTweets;
    private int currentSentimentTweets;

    private long start_time;
    
    public TaskStatus(int tweetsCount, String uuid) {
        this.totalEntitiesTweets = tweetsCount;
        this.totalSentimentTweets = tweetsCount;
        this.currentEntitiesTweets = 0;
        this.currentSentimentTweets = 0;
        this.start_time = new DateTime().getMillis();
    }

    public void incrementEntitiesCount(){
        this.currentEntitiesTweets++;
    }

    public void incrementSentimentCount(){
        this.currentSentimentTweets++;
    }

    public boolean isDone(){
        return ((currentSentimentTweets == totalSentimentTweets) &&
                (currentEntitiesTweets == totalEntitiesTweets) );
    }
    
    public long getDuration(){
    	long end_time = new DateTime().getMillis();
    	return end_time - this.start_time;
    }

    public String currentCount() {
        StringBuffer sb = new StringBuffer();
        sb.append(" Sentiments: "+this.currentSentimentTweets+"/"+this.totalSentimentTweets);
        sb.append(" Entities: "+this.currentEntitiesTweets+"/"+this.totalEntitiesTweets);

        return sb.toString();
    }
}
