package com.adgear.anoa.codec.thrift;

import com.adgear.anoa.avro.ThriftDataModified;
import com.adgear.anoa.avro.decode.ThriftDatumTextReader;
import com.adgear.anoa.avro.decode.ValueDecoder;
import com.adgear.anoa.codec.base.SchemalessToAvroBase;
import com.adgear.anoa.provider.Provider;

import org.apache.avro.io.Decoder;
import org.apache.thrift.TBase;
import org.msgpack.type.Value;

/**
 * Transform MessagePack <code>Value</code> objects into Thrift records.
 *
 * @param <T> Type of the Thrift record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.Provider
 * @see com.adgear.anoa.source.schemaless.ValueSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToValue
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToValue
 */
public class ValueToThrift<T extends TBase<T,?>> extends SchemalessToAvroBase<Value,T> {

  /**
   * @param provider    An upstream Provider of MessagePack Value instances.
   * @param thriftClass The class object corresponding to the serialized Thrift records.
   */
  public ValueToThrift(Provider<Value> provider, Class<T> thriftClass) {
    super(provider,
          ThriftDataModified.getModified().getSchema(thriftClass),
          new ThriftDatumTextReader<>(thriftClass));
    reader.withFieldNames();
  }

  @Override
  protected Decoder makeDecoder(Value input) {
    return new ValueDecoder(input);
  }
}
