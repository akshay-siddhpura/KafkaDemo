
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class KafkaConsumerElasticSearchIdempotentAutoCommitFalseBulkRequest {

    public static void main(String[] args) throws IOException {
        String topic ="tweets";
        Logger log = LoggerFactory.getLogger(KafkaConsumerElasticSearchIdempotentAutoCommitFalseBulkRequest.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String> consumer = createKafkaConsumer(topic);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int size = records.count();
            log.info("Number of Records::"+size);
            BulkRequest bulkRequest= new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //String id = record.topic()+"_"+record.partition()+"_"+record.offset(); //generic id
                String id="";
                try {
                     id = extractIdFromTweet(record.value());
                }catch(NullPointerException e)
                {

                }
                IndexRequest indexRequest = new IndexRequest("twitter","tweet",id)
                                                  .source(record.value(), XContentType.JSON);
                bulkRequest.add(indexRequest);
                //IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
                //String idFromResponse = indexResponse.getId();
                //log.info(idFromResponse);
            }
            BulkResponse bulkResponse=null;
            if(bulkRequest.requests().size()>0)
                 bulkResponse =client.bulk(bulkRequest,RequestOptions.DEFAULT);
            consumer.commitSync();
            log.info("Records offsets commited");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //client.close();
    }
    private static JsonParser jsonParser= new JsonParser();
    private static String extractIdFromTweet(String record) throws NullPointerException{
        return jsonParser.parse(record).getAsJsonObject().get("id_str").getAsString();
    }

    public static RestHighLevelClient createClient() {
        String hostname = ""; // localhost or bonsai url
        String username = ""; // needed only for bonsai
        String password = ""; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String,String> createKafkaConsumer(String topic)
    {
        String bootstrapServer = "akshay-Ubuntu:9092";
        String groupId ="elastic-search";
        //String topic ="tweets";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        KafkaConsumer<String,String> consumer= new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
