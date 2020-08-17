package cn.yintech.test;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
//        props.put("bootstrap.servers", "39.107.113.198:9092,59.110.168.230:9092,47.95.237.41:9092");
        props.put("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092");
        props.put("group.id", "test016");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("sc_md"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            System.out.println("count:" + records.count());
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s , time = %d%n ", record.offset(), record.key(), record.value() , record.timestamp());
        }
    }

}