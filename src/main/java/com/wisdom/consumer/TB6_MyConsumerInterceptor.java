package com.wisdom.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static java.util.stream.Collectors.toList;

/**
 * Created by BaldKiller
 * on 2019/8/18 23:12
 */
public class TB6_MyConsumerInterceptor implements ConsumerInterceptor<String,String> {
    /*
    *   对收到的数据进行判断，如果values不等于10 就不要
    *
    * */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        // 1、定义最终要返回的值
        Map<TopicPartition, List<ConsumerRecord<String, String>>> results = new HashMap<>();
        // 2、遍历partition
        Set<TopicPartition> partitions = records.partitions();
        // 3、遍历partition中的record
        partitions.forEach(p->{
            List<ConsumerRecord<String, String>> result = records.records(p)
                    .stream().filter(record -> record.value().equals("HELLO 10"))
                    .collect(toList());
            results.put(p,result);
        });
        return new ConsumerRecords<>(results);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        System.out.println("============begin");
        System.out.println(offsets);
        System.out.println("============end");
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
