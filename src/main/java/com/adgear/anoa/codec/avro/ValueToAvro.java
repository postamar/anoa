package com.adgear.anoa.codec.avro;

import com.adgear.anoa.avro.decode.ValueDecoder;
import com.adgear.anoa.codec.base.SchemalessToAvroBase;
import com.adgear.anoa.provider.Provider;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.msgpack.type.Value;

/**
 * Transform MessagePack <code>Value</code> objects into Avro records.
 *
 * @param <R> Type of the Avro record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.source.schemaless.ValueSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToValue
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToValue
 */
public class ValueToAvro<R extends IndexedRecord> extends SchemalessToAvroBase<Value, R> {

  /**
   * Constructs a SpecificRecord Provider.
   *
   * @param recordClass class object of SpecificRecord implementation.
   */
  public ValueToAvro(Provider<Value> provider, Class<R> recordClass) {
    super(provider, recordClass);
    reader.withFieldNames();
  }

  /**
   * Constructs a GenericRecord Provider.
   */
  public ValueToAvro(Provider<Value> provider, Schema recordSchema) {
    super(provider, recordSchema);
    reader.withFieldNames();
  }

  @Override
  protected Decoder makeDecoder(Value input) {
    return new ValueDecoder(input);
  }
}
