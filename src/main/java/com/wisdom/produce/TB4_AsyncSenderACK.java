package com.wisdom.produce;

import com.wisdom.util.PropertiesUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by BaldKiller
 * on 2019/4/17 9:40
 * ack 1 = 35626
 * ack 0 = 32434
 * ack all = 29599
 */
public class TB4_AsyncSenderACK {
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.getProperties("producer");

        // 创建KafkaProducer 并且传入配置
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        long start = System.currentTimeMillis();
        // 创建producerRecoder
        for (int i = 0; i < 100000; i++) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first","hello"+i);

            // 通过异步的方式发送
            // 不管是否发送成功 一直发送 发送成功或失败会调用回调函数
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        System.out.println("offset : "+metadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.flush();
        producer.close();
        long current = System.currentTimeMillis();
        System.out.println(current-start);

    }
}
