import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import ta.utils.QueueNames;
import ta.utils.SQSUtil;






import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

/**
 * Created by arielbaruch on 07/12/14.
 */
public class Worker {

    private SQSUtil sqsUtil;

    private UUID uuid;
    private String ID;
    private StanfordCoreNLP  sentimentPipeline;
    private StanfordCoreNLP NERPipeline;

    public Worker(SQSUtil sqsUtil) {
        this.sqsUtil = sqsUtil;
        this.uuid = UUID.randomUUID();
        this.ID = "WA-" + uuid.toString().substring(0,3);
        
        Properties sentiment_props = new Properties();
        sentiment_props.put("annotators", "tokenize, ssplit, parse, sentiment");
        this.sentimentPipeline =  new StanfordCoreNLP(sentiment_props);
        
        Properties entities_props = new Properties();
        entities_props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        this.NERPipeline =  new StanfordCoreNLP(entities_props);
        
    }

    public void start() {
        while (true){
        	Message msg = sqsUtil.receiveMessage(QueueNames.RAW_TWEETS_QUEUE_NAME);
        	
            if(msg != null){
            	Map<String, MessageAttributeValue> attrs = msg.getMessageAttributes();
            	
                processMessage(msg,attrs);
                sendMessage(msg,attrs);
                sqsUtil.deleteMessage(QueueNames.RAW_TWEETS_QUEUE_NAME, msg);
                
           }
        }
    }
    

    public void processMessage(Message msg, Map<String, MessageAttributeValue> attrs){
        if (msg != null){
        	System.out.print(">");
        	String task = attrs.get("task").getStringValue();
        	
            if (task.equals("sentiment")){
            	attrs.put("sentiment", new MessageAttributeValue().withDataType("String").withStringValue( Integer.toString( findSentiment(msg.getBody()) ) ) );
            	attrs.put("entities", new MessageAttributeValue().withDataType("String").withStringValue( "NA" )  );

            }
            else if (task.equals("entities")){
            	attrs.put("entities", new MessageAttributeValue().withDataType("String").withStringValue( findEntities(msg.getBody()) )  );
            	attrs.put("sentiment", new MessageAttributeValue().withDataType("String").withStringValue(  "NA" ) );


            }
        }
    }

    public void sendMessage(Message processed, Map<String, MessageAttributeValue> attrs){
        sqsUtil.sendMessage(QueueNames.PROCESSED_TWEETS_QUEUE_NAME, processed.getBody(), attrs);
    }

    private int findSentiment(String tweet) {
        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

    //only PERSON, LOCATION, ORGANIZATION
    private String findEntities(String tweet){
        StringBuilder entities = new StringBuilder();
        entities.append("[");
        
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);

                if (ne.equals("ORGANIZATION") || ne.equals("PERSON") || ne.equals("LOCATION")){
                    entities.append(word + " : " + ne + ", ");
                }
            }
        }
        
        entities.append("]");

        return  entities.toString();
    }

    public String getID() {
        return ID;
    }
}
