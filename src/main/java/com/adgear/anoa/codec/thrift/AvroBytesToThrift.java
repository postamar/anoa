package com.adgear.anoa.codec.thrift;

import com.adgear.anoa.avro.ThriftDataModified;
import com.adgear.anoa.codec.base.AvroDeserializerBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;

import org.apache.avro.Schema;
import org.apache.avro.thrift.ThriftDatumReader;
import org.apache.thrift.TBase;

/**
 * Transforms Avro binary reprensentations into Thrift objects.
 *
 * @param <T> Type of the Thrift record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.codec.serialized.AvroGenericToBytes
 * @see com.adgear.anoa.codec.serialized.AvroSpecificToBytes
 */
public class AvroBytesToThrift<T extends TBase<T,?>> extends AvroDeserializerBase<T> {

  /**
   * Construct codec assuming identical reader and writer Avro Schemas.
   *
   * @param thriftClass A class object from which both reader and writer Avro Schemas are inferred.
   */
  public AvroBytesToThrift(Provider<byte[]> provider, Class<T> thriftClass) {
    this(provider,
         ThriftDataModified.getModified().getSchema(thriftClass),
         ThriftDataModified.getModified().getSchema(thriftClass));
  }

  /**
   * Construct codec assuming different reader and writer Avro Schemas.
   *
   * @param provider    An AvroProvider which supplies the writer Schema.
   * @param thriftClass A class object from which the reader Schema is inferred.
   */
  public AvroBytesToThrift(AvroProvider<byte[]> provider, Class<T> thriftClass) {
    this(provider,
         ThriftDataModified.getModified().getSchema(thriftClass),
         provider.getAvroSchema());
  }

  protected AvroBytesToThrift(Provider<byte[]> provider, Schema readerSchema, Schema writerSchema) {
    super(provider,
          readerSchema,
          new ThriftDatumReader<T>(writerSchema, readerSchema));
  }
}
