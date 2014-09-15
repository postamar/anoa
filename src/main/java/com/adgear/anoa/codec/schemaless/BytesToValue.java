package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.codec.base.CodecBase;
import com.adgear.anoa.provider.Provider;

import org.msgpack.MessagePack;
import org.msgpack.type.Value;
import org.msgpack.unpacker.BufferUnpacker;
import org.msgpack.unpacker.MessagePackBufferUnpacker;

import java.io.IOException;

/**
 * Transforms MessagePack serializations into MessagePack <code>Value</code> instances.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.codec.serialized.ValueToBytes
 */
public class BytesToValue extends CodecBase<byte[], Value, BytesToValue.Counter> {

  final private BufferUnpacker unpacker;

  public BytesToValue(Provider<byte[]> provider) {
    super(provider, Counter.class);
    this.unpacker = new MessagePackBufferUnpacker(new MessagePack());
  }

  @Override
  public Value transform(byte[] input) {
    if (input.length == 0) {
      increment(Counter.EMPTY_RECORD);
      return null;
    }
    try {
      return unpacker.feed(input).readValue();
    } catch (IOException e) {
      logger.warn(e.getMessage());
      increment(Counter.MSGPACK_DESERIALIZE_FAIL);
      return null;
    }
  }

  static public enum Counter {
    /**
     * Counts number of records which could not be deserialized.
     */
    MSGPACK_DESERIALIZE_FAIL,

    /**
     * Counts the number of times the input records was empty.
     */
    EMPTY_RECORD
  }
}
