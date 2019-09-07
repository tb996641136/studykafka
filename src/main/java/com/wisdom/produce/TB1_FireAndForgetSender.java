package com.wisdom.produce;

import com.wisdom.util.PropertiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by BaldKiller
 * on 2019/4/17 9:40
 */
public class TB1_FireAndForgetSender {
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.getProperties("producer");

        // 创建KafkaProducer 并且传入配置
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 创建producerRecoder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test_serializer","hello"+i);
            producer.send(producerRecord);
        }
        producer.flush();
        producer.close();

    }

}
