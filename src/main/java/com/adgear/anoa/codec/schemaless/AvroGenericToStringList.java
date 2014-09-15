package com.adgear.anoa.codec.schemaless;

import com.adgear.anoa.avro.encode.GenericDatumTextWriter;
import com.adgear.anoa.avro.encode.StringListEncoder;
import com.adgear.anoa.codec.base.AvroToSchemalessBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

/**
 * Transforms Avro GenericRecords into String lists, when possible. <p> In other words, requires a
 * flat record schema. Value ordering follows Schema field ordering.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.avro.AvroSource
 * @see com.adgear.anoa.codec.avro.BytesToAvroGeneric
 * @see com.adgear.anoa.codec.avro.JsonNodeToAvro
 * @see com.adgear.anoa.codec.avro.StringListToAvro
 * @see com.adgear.anoa.codec.avro.ValueToAvro
 */
public class AvroGenericToStringList extends AvroToSchemalessBase<GenericRecord, List<String>> {

  public AvroGenericToStringList(AvroProvider<GenericRecord> provider) {
    this(provider, provider.getAvroSchema());
  }

  /**
   * @param provider     To be used as upstream Provider.
   * @param recordSchema The Schema of the records returned by the given upstream Provider.
   */
  public AvroGenericToStringList(Provider<GenericRecord> provider, Schema recordSchema) {
    super(provider,
          recordSchema,
          new StringListEncoder(),
          new GenericDatumTextWriter<GenericRecord>(recordSchema));
  }
}
