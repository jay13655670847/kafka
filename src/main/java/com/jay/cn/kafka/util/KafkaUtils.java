package com.jay.cn.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * @author lj
 * @version 1.0
 * @date 2021/9/25 15:25
 *kafka工具类
 */
public class KafkaUtils {

    public static final String KAKFA_IP="192.168.216.129:9092,192.168.216.130:9092,192.168.216.131:9092";

    public KafkaConsumer<String, String> getKafkaConsumer(){
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAKFA_IP);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //消费的细节
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ooxx");
        //KAKFA IS MQ  IS STORAGE
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//第一次启动，米有offset
        /**
         *         "What to do when there is no initial offset in Kafka or if the current offset
         *         does not exist any more on the server
         *         (e.g. because that data has been deleted):
         *         <ul>
         *             <li>earliest: automatically reset the offset to the earliest offset
         *             <li>latest: automatically reset the offset to the latest offset</li>
         *             <li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li>
         *         </ul>";
         */
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");//自动提交时异步提交，丢数据&&重复数据
        //一个运行的consumer ，那么自己会维护自己消费进度
        //一旦你自动提交，但是是异步的
        //1，还没到时间，挂了，没提交，重起一个consuemr，参照offset的时候，会重复消费
        //2，一个批次的数据还没写数据库成功，但是这个批次的offset背异步提交了，挂了，重起一个consuemr，参照offset的时候，会丢失消费

//        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"15000");//5秒
//        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,""); // POLL 拉取数据，弹性，按需，拉取多少？

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(p);

        return consumer;
    }

    public KafkaProducer<String, String> getKafkaProducer(){
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAKFA_IP);
        //kafka  持久化数据的MQ  数据-> byte[]，不会对数据进行干预，双方要约定编解码
        //kafka是一个app：：使用零拷贝  sendfile 系统调用实现快速数据消费
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.ACKS_CONFIG, "-1");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);
        return producer;
    }

    @Test
    public void testConsumer(){
        KafkaConsumer<String, String> kafkaConsumer = this.getKafkaConsumer();

        //订阅主题
        kafkaConsumer.subscribe(Arrays.asList("ooxx"));

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(">>>>>>>>>>>>>>>topic = " + record.topic() + " offset = " + record.offset() + " value = " + record.value());
            }
        }
    }

    @Test
    public void testProducer() throws InterruptedException {
        KafkaProducer<String, String> kafkaProducer = this.getKafkaProducer();

        String topic="ooxx";

        ProducerRecord producerRecord = new ProducerRecord(topic, UUID.randomUUID().toString());

        while (true){
            Future future = kafkaProducer.send(producerRecord);
            Thread.sleep(1000);
        }

    }
}
