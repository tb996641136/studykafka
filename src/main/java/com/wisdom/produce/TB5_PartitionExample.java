package com.wisdom.produce;

import com.wisdom.util.PropertiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by BaldKiller
 * on 2019/7/30 22:47
 */
public class TB5_PartitionExample {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = PropertiesUtil.getProperties("producer");

        /*
        *   不指定key会随机发送到某个partition
        * */
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test_p","hello");
        Future<RecordMetadata> future = producer.send(producerRecord);
        RecordMetadata recordMetadata = future.get();
        System.out.println("partition - "+ recordMetadata.partition());

        producerRecord = new ProducerRecord<>("test_p","hello");
        future = producer.send(producerRecord);
        recordMetadata = future.get();
        System.out.println("partition - "+ recordMetadata.partition());

        /*
        *   指定key，会根据key的hash值模上分区数得到一个partition
        *   相同的key会发送到相同的patition里面
        * */
        producerRecord = new ProducerRecord<>("test_p","hello","word");
        future = producer.send(producerRecord);
        recordMetadata = future.get();
        System.out.println("partitionkey - "+ recordMetadata.partition());

        producerRecord = new ProducerRecord<>("test_p","hello","word");
        future = producer.send(producerRecord);
        recordMetadata = future.get();
        System.out.println("partitionkey - "+ recordMetadata.partition());



        producer.flush();
        producer.close();
    }
}
