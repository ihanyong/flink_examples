package com.wwware.flink.test.stock;

/**
 * CanalMqMessageNoTransformDeserializationSchema
 *
 * @author yong.han
 * 2019/1/24
 */

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class CanalMqMessageNoTransformDeserializationSchema implements DeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) throws IOException {
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}
