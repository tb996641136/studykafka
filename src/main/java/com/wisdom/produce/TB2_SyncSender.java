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
 * on 2019/4/17 9:40
 * 同步方式进行发送数据
 */
public class TB2_SyncSender {
    public static void main(String[] args) {
        Properties properties = PropertiesUtil.getProperties("producer");
        //properties.put("max.in.flight.requests.per.connection","3");
        // 创建KafkaProducer 并且传入配置
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 创建producerRecoder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first","hello"+i);

            // send 后会等待服务器返回 只有返回成功才会发下一个
            Future<RecordMetadata> future = producer.send(producerRecord);

            try {
                // future.get 会阻塞住
                // matadata 会有发送到什么partition offset等信息
                RecordMetadata metadata = future.get();
                System.out.println("offset - "+metadata.offset()+",partition - "+metadata.partition()+",topic - "+metadata.topic());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
        producer.flush();
        producer.close();

    }

}
