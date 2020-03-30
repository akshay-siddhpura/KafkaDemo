import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsDemo
{
    public static void main(String[] args) {
        String bootstrapServer = "akshay-Ubuntu:9092";
        String applicationId ="elastic-search";
        Properties properties= new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());


        //create topology
        StreamsBuilder streamsBuilder= new StreamsBuilder();

        //input topic
        KStream<String,String> inputStream =  streamsBuilder.stream("tweets");
        KStream<String,String> filteredStream =inputStream.filter(
                (k,v)-> extractNumberOfFollowers(v)>10000
        );
        filteredStream.to("filter_stream");
        KafkaStreams kafkaStreams= new KafkaStreams(streamsBuilder.build(),properties);
        kafkaStreams.start();
    }

    private static JsonParser jsonParser= new JsonParser();
    private static Integer extractNumberOfFollowers(String record) {
        Integer count=0;
        try {
           count = jsonParser.parse(record).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
           //System.out.println(count);
        }
        catch (NullPointerException e)
        {
            return  0;
        }
        return count;
    }
}
