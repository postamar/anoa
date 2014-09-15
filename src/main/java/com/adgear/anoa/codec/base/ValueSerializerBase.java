package com.adgear.anoa.codec.base;

import com.adgear.anoa.provider.Provider;

import org.msgpack.packer.BufferPacker;
import org.msgpack.type.Value;

import java.io.IOException;

/**
 * Base class for serializing MessagePack <code>Value</code> objects.
 */
abstract public class ValueSerializerBase
    extends CodecBase<Value, byte[], ValueSerializerBase.Counter> {

  final private BufferPacker packer;

  protected ValueSerializerBase(Provider<Value> provider, BufferPacker packer) {
    super(provider, Counter.class);
    this.packer = packer;
  }

  @Override
  public byte[] transform(Value input) {
    try {
      packer.clear();
      packer.write(input);
      packer.flush();
      return packer.toByteArray();
    } catch (IOException e) {
      increment(Counter.MSGPACK_SERIALIZATION_FAIL);
      logger.warn(e.getMessage());
      return null;
    }
  }

  static public enum Counter {
    /**
     * Counts the number of times the MessagePack BufferPacker failed to serialize a record.
     */
    MSGPACK_SERIALIZATION_FAIL
  }
}
