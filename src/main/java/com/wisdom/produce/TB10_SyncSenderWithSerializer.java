package com.wisdom.produce;

import com.wisdom.consumer.internal.User;
import com.wisdom.util.PropertiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by BaldKiller
 * on 2019/4/17 9:40
 * 同步方式进行发送数据
 */
public class TB10_SyncSenderWithSerializer {
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.getProperties("producer");

        // 创建KafkaProducer 并且传入配置
        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);

        // 创建producerRecoder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,User> producerRecord = new ProducerRecord<>("test_serializer",new User(i,"tianbo"+i,"新疆"));

            // send 后会等待服务器返回 只有返回成功才会发下一个
            Future<RecordMetadata> future = producer.send(producerRecord);

        }
        producer.flush();
        producer.close();

    }

}
