package com.wwware.flink.test.stock;

import com.alibaba.otter.canal.protocol.Message;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * CanalMqMessageDeserializationSchema
 *
 * @author yong.han
 * 2019/1/24
 */
public class CanalMqMessageDeserializationSchema implements DeserializationSchema<Message> {
    @Override
    public Message deserialize(byte[] message) throws IOException {
        return OwnerStock.deserializer.deserialize(null, message);
    }

    @Override
    public boolean isEndOfStream(Message nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return TypeInformation.of(Message.class);
    }
}
