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
 * on 2019/7/30 23:32
 */
public class TB7_TestPartitioner {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = PropertiesUtil.getProperties("producer");
        // 如果要使用我们自己定义的partitioner就要指定下面的参数
        properties.put("partitioner.class","com.wisdom.produce.TB6_MyPartitioner");

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test_p","hello");
        Future<RecordMetadata> future = producer.send(producerRecord);
        RecordMetadata recordMetadata = future.get();
        System.out.println("partition - "+ recordMetadata.partition());
    }
}
