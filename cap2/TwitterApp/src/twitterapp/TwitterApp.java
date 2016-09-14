package twitterapp;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterApp {
    
    private ConfigurationBuilder cb;
    private DB banco;
    private DBCollection collection;
        
    public void capturaTweets() 
            throws InterruptedException {
        
        TwitterStream twitterStream = 
                new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                BasicDBObject obj = new BasicDBObject();
                obj.put("tweet_ID", status.getId());
                obj.put("usuario", status.getUser().getScreenName());
                obj.put("tweet", status.getText());

                try {
                    collection.insert(obj);
                } catch (Exception e) {
                    System.out.println("Erro de conexão: " 
                            + e.getMessage());

                }
            }      

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
                throw new UnsupportedOperationException("Not supported yet."); 
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                throw new UnsupportedOperationException("Not supported yet."); 
            }

            @Override
            public void onScrubGeo(long l, long l1) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public void onStallWarning(StallWarning sw) {
                throw new UnsupportedOperationException("Not supported yet."); 
            }

            @Override
            public void onException(Exception excptn) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
        
        String keywords[] = {"bigcompras"};
        FilterQuery fq = new FilterQuery();
        fq.track(keywords);
        twitterStream.addListener(listener);
        twitterStream.filter(fq);
    }
    
    public  void conectaMongoDB(){
        try {
            Mongo mongoCli;
            mongoCli = new MongoClient("127.0.0.1");
            banco = mongoCli.getDB("twDB");
            collection = banco.getCollection("tweets");
            BasicDBObject index = new BasicDBObject("tweet_ID", 1);
            collection.ensureIndex(index, new BasicDBObject("unique", true));
        } catch (UnknownHostException | MongoException ex) {
            System.out.println("MongoException :" + ex.getMessage());
        }
    }
    
    
    public void configuraCredenciais(){
        cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("XXXXXXXXXXXXXXXXX");
        cb.setOAuthConsumerSecret("XXXXXXXXXXXXXXXXX");
        cb.setOAuthAccessToken("XXXXXXXXXXXXXXXXX");
        cb.setOAuthAccessTokenSecret("XXXXXXXXXXXXXXXXX");        
    }
   
    
    public static void main(String[] args) throws InterruptedException { 
        TwitterApp tw = new TwitterApp();
        tw.conectaMongoDB();
        tw.configuraCredenciais();
        tw.capturaTweets();   
    }

}