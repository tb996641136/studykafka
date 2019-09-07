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
 * 使用异步的方式提交offset
 * 异步的方式提交offset时，异步提交的时候不会Block住，会立即返回，也不会进行retry
 * 调用callback接口，可以查看是否提交成功，
 */
public class TB3_ConsumerAsyncCommit {
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

            }
            
            /*
            *   返回的callback里面包含一个map和一个exception
            *   map里面有topic partition 和 offset的一些信息
            *   如果exception不为Null就说明提交offset失败了
            * */
            consumer.commitAsync((map,e)->{
                if(e != null){
                    e.printStackTrace();
                }else {
                    System.out.println(map);
                }
            });

        }
    }
}
