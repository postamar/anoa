package com.adgear.anoa.codec.thrift;

import com.adgear.anoa.avro.ThriftDataModified;
import com.adgear.anoa.avro.decode.StringListDecoder;
import com.adgear.anoa.avro.decode.ThriftDatumTextReader;
import com.adgear.anoa.codec.base.SchemalessToAvroBase;
import com.adgear.anoa.provider.Provider;

import org.apache.avro.io.Decoder;
import org.apache.thrift.TBase;

import java.util.List;

/**
 * Transform String list objects into Thrift records.
 *
 * @param <T> Type of the Thrift record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.Provider
 * @see com.adgear.anoa.source.schemaless.CsvSource
 * @see com.adgear.anoa.source.schemaless.CsvWithHeaderSource
 * @see com.adgear.anoa.source.schemaless.TsvSource
 * @see com.adgear.anoa.source.schemaless.TsvWithHeaderSource
 * @see com.adgear.anoa.source.schemaless.JdbcSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToStringList
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToStringList
 */
public class StringListToThrift<T extends TBase<T,?>> extends SchemalessToAvroBase<List<String>,T> {

  /**
   * @param provider    An upstream Provider of string lists.
   * @param thriftClass The class object corresponding to the serialized Thrift records.
   */
  public StringListToThrift(Provider<List<String>> provider, Class<T> thriftClass) {
    super(provider,
          ThriftDataModified.getModified().getSchema(thriftClass),
          new ThriftDatumTextReader<>(thriftClass));
  }

  @Override
  protected Decoder makeDecoder(List<String> input) {
    return new StringListDecoder(input);
  }
}
