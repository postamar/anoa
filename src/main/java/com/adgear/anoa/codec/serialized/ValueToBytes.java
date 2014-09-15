package com.adgear.anoa.codec.serialized;

import com.adgear.anoa.codec.base.ValueSerializerBase;
import com.adgear.anoa.provider.Provider;

import org.msgpack.MessagePack;
import org.msgpack.packer.MessagePackBufferPacker;
import org.msgpack.type.Value;

/**
 * Transforms MessagePack <code>Value</code> objects into MessagePack serializations.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.schemaless.ValueSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToValue
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToValue
 * @see com.adgear.anoa.codec.schemaless.BytesToValue
 */
public class ValueToBytes extends ValueSerializerBase {

  public ValueToBytes(Provider<Value> provider) {
    super(provider, new MessagePackBufferPacker(new MessagePack()));
  }
}
