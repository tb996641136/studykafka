package com.wisdom.consumer;

import com.wisdom.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by BaldKiller
 * on 2019/7/31 23:46
 */
public class TB1_SimpleConsumer {
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.getProperties("consumer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singletonList("test_p"));
        for(;;){
            // 会从topic中拉数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record : records){
                // 这里一般会进行消息处理
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

        }
    }
}
