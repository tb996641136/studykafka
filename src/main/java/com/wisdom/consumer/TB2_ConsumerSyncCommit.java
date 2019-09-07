package com.wisdom.consumer;

import com.wisdom.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by BaldKiller
 * on 2019/7/31 23:46
 * 使用同步的方式提交offset
 * 同步的方式提交offset时，会Block住，直到提交offset成功，或者提交失败超过retry的次数
 */
public class TB2_ConsumerSyncCommit {
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.getProperties("consumer");
        // 关闭自动提交offset
        properties.put("enable.auto.commit","false");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singletonList("test_p"));
        for(;;){
            // 会从topic中拉数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record : records){
                // 这里一般会进行消息处理
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

                // 在这里就是 处理完一条数据就进行commit offset
                // consumer.commitSync();
            }

            // 处理完这一批数据在commit offset
            consumer.commitSync();

        }
    }
}
