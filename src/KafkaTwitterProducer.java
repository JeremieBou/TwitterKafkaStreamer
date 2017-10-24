
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jeremie.
 */
public class KafkaTwitterProducer {
    public static final String INPUT_TOPIC = "twitter;,abjkpqrtw";
    public static final String BROKER_LIST = "localhost:9092,localhost:9093";

    public static void main(String[] args) throws Exception {
        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<>(1000);

        if(args.length < 4) {
            System.out.println("put your stuff");

            return;
        }

        String consumerKey = args[0].toString();
        String consumerSecret = args[1].toString();
        String accessToken = args[2].toString();
        String accessTokenSecret = args[3].toString();

        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 5 , arguments.length);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey(consumerKey)
            .setOAuthConsumerSecret(consumerSecret)
            .setOAuthAccessToken(accessToken)
            .setOAuthAccessTokenSecret(accessTokenSecret);

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
            }
        };

        twitterStream.addListener(listener);

        FilterQuery query = new FilterQuery().track(keyWords);
        twitterStream.filter(query);

        Thread.sleep(5000);

        Producer<String, String> producer = new KafkaProducer<>(getProducerProperties(BROKER_LIST));
        int i = 0;
        int j = 0;

        while(i < 10){
            Status ret = queue.poll();

            if(ret == null){
                Thread.sleep(100);
            } else {
                for(HashtagEntity hashtag : ret.getHashtagEntities()){
                    System.out.println("Hashtag: " + hashtag.getText());
                    producer.send(new ProducerRecord<String, String>(
                            INPUT_TOPIC, Integer.toString(j++), hashtag.getText()
                    ));
                }
            }
        }

        producer.close();
        Thread.sleep(5000);
        twitterStream.shutdown();
    }

    private static Properties getProducerProperties(String bootstrapServers) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
}
