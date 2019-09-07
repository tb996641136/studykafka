package com.wisdom.produce;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by BaldKiller
 * on 2019/8/18 22:59
 */
public class TB8_MyProducerInterceptor implements ProducerInterceptor<String,String> {
    /*
    *   将record中的Values变为大写
    *
    *   下面为recoder最全的构造参数
    *   public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers)
    *
    * */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<>(record.topic(),record.partition(),record.timestamp(),
                record.key(),record.value().toUpperCase(),record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
