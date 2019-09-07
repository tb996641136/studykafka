package com.wisdom.produce;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created by BaldKiller
 * on 2019/7/30 23:21
 */
public class TB6_MyPartitioner implements Partitioner {
    private static final String LOGIN = "LOGIN";
    private static final String LOGOUT = "LOGOUT";
    private static final String ORDER = "ORDER";

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null || keyBytes.length == 0)
            throw new IllegalArgumentException("key must be set!");

        switch (key.toString().toUpperCase()) {
            case LOGIN:
                return 0;
            case LOGOUT:
                return 1;
            case ORDER:
                return 2;
            default:
                throw new IllegalArgumentException("The key is invalid");
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
