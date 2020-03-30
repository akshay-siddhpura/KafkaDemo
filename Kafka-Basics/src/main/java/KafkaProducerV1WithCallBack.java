import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerV1WithCallBack {

    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(KafkaProducerV1WithCallBack.class);
        //create producer properties
         String bootStrapServers= "akshay-Ubuntu:9092";
//        properties.setProperty("bootstrap.servers",bootStrapServers);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer",StringSerializer.class.getName());
         Properties properties = new Properties();
         properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
         properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
         properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

         //create producer
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);

        for(int i =0;i<100;i++)
        {
            ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","Hi Akshay_"+ i);
            //send data-async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time when record is successfully sent or exception is thrown
                    if(e==null)
                    {
                        log.info("MetaData \n"+
                                "Topic ::"+recordMetadata.topic() + "\n"+
                                "Partition ::"+recordMetadata.partition() + "\n"+
                                "offset ::"+recordMetadata.offset() + "\n"+
                                "timestamp ::"+recordMetadata.timestamp());

                    }
                    else {
                        log.error("Error while producing",e);
                    }

                }
            });
        }
        producer.flush();
        producer.close();


    }
}
