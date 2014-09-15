package com.adgear.anoa.codec.serialized;

import com.adgear.anoa.codec.base.ValueSerializerBase;
import com.adgear.anoa.provider.Provider;

import org.msgpack.MessagePack;
import org.msgpack.type.Value;
import org.msgpack.util.json.JSONBufferPacker;

/**
 * Transforms MessagePack <code>Value</code> objects into JSON serializations.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.schemaless.ValueSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToValue
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToValue
 */
public class ValueToJsonBytes extends ValueSerializerBase {

  public ValueToJsonBytes(Provider<Value> provider) {
    super(provider, new JSONBufferPacker(new MessagePack()));
  }

}