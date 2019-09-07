package com.wisdom.consumer;

import com.wisdom.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by BaldKiller
 * on 2019/7/31 23:46
 * 提交指定partition的offset
 */
public class TB5_CommitSpecifiedOffset {
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.getProperties("consumer");
        // 关闭自动提交offset
        properties.put("enable.auto.commit","false");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // 订阅topic
        consumer.subscribe(Collections.singletonList("test_p"));


        Map<TopicPartition,OffsetAndMetadata> offset = new HashMap<>();
        TopicPartition tp = new TopicPartition("test12",1);
        OffsetAndMetadata om = new OffsetAndMetadata(14,"no metadata");
        offset.put(tp,om);

        for(;;){
            // 会从topic中拉数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record : records){
                // 这里一般会进行消息处理
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

            }
            consumer.commitSync(offset);


        }
    }
}
