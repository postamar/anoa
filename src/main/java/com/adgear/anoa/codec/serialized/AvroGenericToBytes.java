package com.adgear.anoa.codec.serialized;

import com.adgear.anoa.codec.base.AvroSerializerBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/**
 * Transforms Avro GenericRecords into Avro binary serializations.
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.source.avro.AvroSource
 * @see com.adgear.anoa.codec.avro.BytesToAvroGeneric
 * @see com.adgear.anoa.codec.avro.JsonNodeToAvro
 * @see com.adgear.anoa.codec.avro.StringListToAvro
 * @see com.adgear.anoa.codec.avro.ValueToAvro
 */
public class AvroGenericToBytes extends AvroSerializerBase<GenericRecord> {

  public AvroGenericToBytes(AvroProvider<GenericRecord> provider) {
    this(provider, provider.getAvroSchema());
  }

  /**
   * @param provider     To be used as upstream Provider.
   * @param recordSchema The Schema of the records returned by the given upstream Provider.
   */
  public AvroGenericToBytes(Provider<GenericRecord> provider,
                            Schema recordSchema) {
    super(provider,
          recordSchema,
          new GenericDatumWriter<GenericRecord>(recordSchema));
  }

}
