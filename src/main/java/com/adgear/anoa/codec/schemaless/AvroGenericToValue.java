package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.avro.encode.GenericDatumTextWriter;
import com.adgear.anoa.avro.encode.ValueEncoder;
import com.adgear.anoa.codec.base.AvroToSchemalessBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.msgpack.type.Value;

/**
 * Transforms Avro GenericRecords into MessagePack <code>Value</code> instances.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.avro.AvroSource
 * @see com.adgear.anoa.codec.avro.BytesToAvroGeneric
 * @see com.adgear.anoa.codec.avro.JsonNodeToAvro
 * @see com.adgear.anoa.codec.avro.StringListToAvro
 * @see com.adgear.anoa.codec.avro.ValueToAvro
 */
public class AvroGenericToValue extends AvroToSchemalessBase<GenericRecord, Value> {

  public AvroGenericToValue(AvroProvider<GenericRecord> provider) {
    this(provider, provider.getAvroSchema());
  }

  /**
   * @param provider     To be used as upstream Provider.
   * @param recordSchema The Schema of the records returned by the given upstream Provider.
   */
  public AvroGenericToValue(Provider<GenericRecord> provider, Schema recordSchema) {
    super(provider,
          recordSchema,
          new ValueEncoder(),
          new GenericDatumTextWriter<GenericRecord>(recordSchema));
    writer.withFieldNames();
  }
}
