package com.adgear.anoa.codec.avro;

import com.adgear.anoa.codec.base.AvroDeserializerBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

/**
 * Transforms byte arrays containing serialized Avro records into SpecificRecord instances
 *
 * @param <R> Type of the Avro SpecificRecord to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.codec.serialized.AvroGenericToBytes
 * @see com.adgear.anoa.codec.serialized.AvroSpecificToBytes
 */
public class BytesToAvroSpecific<R extends SpecificRecord> extends AvroDeserializerBase<R> {

  /**
   * Construct codec assuming identical reader and writer Avro Schemas.
   *
   * @param provider An AvroProvider which provides both reader and writer Schema.
   */
  public BytesToAvroSpecific(AvroProvider<byte[]> provider) {
    this(provider, provider.getAvroSchema(), provider.getAvroSchema());
  }

  /**
   * Construct codec assuming different reader and writer Avro Schemas.
   *
   * @param provider    An AvroProvider which provides the writer Schema.
   * @param recordClass the class object whose corresponding Schema will be used as reader.
   */
  public BytesToAvroSpecific(AvroProvider<byte[]> provider,
                             Class<R> recordClass) {
    this(provider,
         SpecificData.get().getSchema(recordClass),
         provider.getAvroSchema());
  }

  /**
   * Construct codec assuming identical reader and writer Avro Schemas.
   *
   * @param recordClass the class object whose corresponding Schema will be used as both reader and
   *                    writer.
   */
  public BytesToAvroSpecific(Provider<byte[]> provider,
                             Class<R> recordClass) {
    super(provider,
          SpecificData.get().getSchema(recordClass),
          new SpecificDatumReader<>(recordClass));
  }

  /**
   * Construct codec assuming different reader and writer Avro Schemas.
   *
   * @param recordClass the class object whose corresponding Schema will be used as reader.
   */
  public BytesToAvroSpecific(Provider<byte[]> provider,
                             Class<R> recordClass,
                             Schema writerSchema) {
    this(provider,
         SpecificData.get().getSchema(recordClass),
         writerSchema);
  }

  /**
   * Construct codec assuming different reader and writer Avro Schemas.
   */
  public BytesToAvroSpecific(Provider<byte[]> provider,
                             Schema readerSchema,
                             Schema writerSchema) {
    super(provider,
          readerSchema,
          new SpecificDatumReader<R>(writerSchema, readerSchema));
  }
}
