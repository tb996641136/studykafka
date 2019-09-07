package com.wisdom.consumer.internal;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by BaldKiller
 * on 2019/8/13 0:12
 */
public class UserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        if (data.length < 12 )
            throw new SerializationException("User lenght must < 12");

        byte[] nameByte;
        byte[] addressByte;
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int id = buffer.getInt();

        int nameByteLenght = buffer.getInt();
        nameByte = new byte[nameByteLenght];
        buffer.get(nameByte);
        String name = new String(nameByte);

        int addressByteLenght = buffer.getInt();
        addressByte = new byte[addressByteLenght];
        buffer.get(addressByte);
        String address = new String(addressByte);

        return new User(id,name,address);
    }

    @Override
    public void close() {

    }
}
