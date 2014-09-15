package com.adgear.anoa.codec.avro;

import com.adgear.anoa.codec.base.AvroDeserializerBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

/**
 * Transforms byte arrays containing serialized Avro records into GenericRecord instances
 *
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.codec.serialized.AvroGenericToBytes
 * @see com.adgear.anoa.codec.serialized.AvroSpecificToBytes
 */
public class BytesToAvroGeneric extends AvroDeserializerBase<GenericRecord> {

  /**
   * Construct codec assuming identical reader and writer Avro Schemas.
   *
   * @param provider An AvroProvider which provides both reader and writer Schema.
   */
  public BytesToAvroGeneric(AvroProvider<byte[]> provider) {
    this(provider, provider.getAvroSchema());
  }

  /**
   * Construct codec assuming different reader and writer Avro Schemas.
   *
   * @param provider An AvroProvider which provides the writer Schema.
   * @param schema   The reader Schema.
   */
  public BytesToAvroGeneric(AvroProvider<byte[]> provider,
                            Schema schema) {
    this(provider, schema, provider.getAvroSchema());
  }

  /**
   * Construct codec assuming identical reader and writer Avro Schemas.
   *
   * @param schema Schema, both reader and writer.
   */
  public BytesToAvroGeneric(Provider<byte[]> provider,
                            Schema schema) {
    super(provider,
          schema,
          new GenericDatumReader<GenericRecord>(schema));
  }

  /**
   * Construct codec assuming different reader and writer Avro Schemas.
   */
  public BytesToAvroGeneric(Provider<byte[]> provider,
                            Schema readerSchema,
                            Schema writerSchema) {
    super(provider,
          readerSchema,
          new GenericDatumReader<GenericRecord>(writerSchema, readerSchema));
  }
}
