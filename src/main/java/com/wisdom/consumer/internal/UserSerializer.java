package com.wisdom.consumer.internal;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by BaldKiller
 * on 2019/8/12 23:57
 */
public class UserSerializer implements Serializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, User data) {
        // 如果为空 就返回空 后续用来做判断
        if (data == null)
            return null;

        int id = data.getId();
        String name = data.getName();
        String address = data.getAddress();

        byte[] nameByte;
        byte[] addressByte;

        if (name != null)
            nameByte = name.getBytes();
        else
            nameByte = new byte[0];

        if (address != null)
            addressByte = address.getBytes();
        else
            addressByte = new byte[0];

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + nameByte.length + 4 + addressByte.length);

        buffer.putInt(id);
        buffer.putInt(nameByte.length);
        buffer.put(nameByte);

        buffer.putInt(addressByte.length);
        buffer.put(addressByte);
        return buffer.array();
    }

    @Override
    public void close() {
        // do nothing
    }
}
