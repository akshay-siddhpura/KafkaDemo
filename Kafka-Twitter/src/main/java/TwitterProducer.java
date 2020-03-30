

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = "";
    String consumerSecret = "";
    String token = "";
    String secret = "";
    List<String> terms = Lists.newArrayList("corona");

    public TwitterProducer() {
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        //twitter client
        Client hosebirdClient =createTwitterClient(msgQueue);
        hosebirdClient.connect();
        //kafka producer
        KafkaProducer<String,String> kafkaTwitterProducer = createKafkaTwitterProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("Stopping application");
            log.info("shutting down  twitter client");
            hosebirdClient.stop();
            log.info("shutting down  kafka producer");
            kafkaTwitterProducer.close();
            log.info("done.");
        }));
        //loop to send tweets kafka producer
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("error while polling",e);
                hosebirdClient.stop();
            }
            if(msg!=null) {
                log.info(msg);
                kafkaTwitterProducer.send(new ProducerRecord<>("tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                      if(e!=null)
                          log.error("something went wrong",e);
                    }
                });
            }
        }
        log.info("End of Application");
    }

    private KafkaProducer<String, String> createKafkaTwitterProducer() {
        String bootStrapServers= "akshay-Ubuntu:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> kafkaTwitterProducer= new KafkaProducer<String, String>(properties);
        return kafkaTwitterProducer;
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
               // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }
}
