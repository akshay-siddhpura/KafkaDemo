import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerV1Threads {

    public static void main(String[] args) {
    new KafkaConsumerV1Threads().run();


    }

    private KafkaConsumerV1Threads()
    {

    }

    private void run()
    {
        String bootstrapServer = "akshay-Ubuntu:9092";
        String groupId = "cg2";
        String topic = "first_topic";
        Logger log = LoggerFactory.getLogger(KafkaConsumerV1Threads.class.getName());
        //latch dealing with multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);
        //create runnable
        Runnable myConsumerThread = new ConsumerThread(countDownLatch,topic,bootstrapServer,groupId);

        //start thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("caught shutdown hook");
            ((ConsumerThread)myConsumerThread).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Applicatin is exited");
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Application Interrupted",e);
        }
        finally {
            log.info("Application is closed");
        }
    }

    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger log = LoggerFactory.getLogger(ConsumerThread.class.getName());
        public ConsumerThread(CountDownLatch latch, String topic, String bootstrapServer, String groupId) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.latch = latch;
            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("record key:" + record.key() + " record value:" + record.value());
                        log.info("partition :" + record.partition() + " offset:" + record.offset());
                    }


                }
            } catch (WakeupException e) {
                    log.info("Received shutdown signal");
            }
            finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            consumer.wakeup();
        }
    }

}
