package com.adgear.anoa.codec.avro;

import com.adgear.anoa.avro.decode.StringListDecoder;
import com.adgear.anoa.codec.base.SchemalessToAvroBase;
import com.adgear.anoa.provider.Provider;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

import java.util.List;

/**
 * Transform String list objects into Avro records.
 *
 * @param <R> Type of the Avro record to be provided by the Codec.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.provider.avro.AvroProvider
 * @see com.adgear.anoa.source.schemaless.CsvSource
 * @see com.adgear.anoa.source.schemaless.CsvWithHeaderSource
 * @see com.adgear.anoa.source.schemaless.TsvSource
 * @see com.adgear.anoa.source.schemaless.TsvWithHeaderSource
 * @see com.adgear.anoa.source.schemaless.JdbcSource
 * @see com.adgear.anoa.codec.schemaless.AvroGenericToStringList
 * @see com.adgear.anoa.codec.schemaless.AvroSpecificToStringList
 */
public class StringListToAvro<R extends IndexedRecord>
    extends SchemalessToAvroBase<List<String>, R> {

  /**
   * Constructs a SpecificRecord Provider.
   *
   * @param recordClass class object of SpecificRecord implementation.
   */
  public StringListToAvro(Provider<List<String>> provider, Class<R> recordClass) {
    super(provider, recordClass);
  }

  /**
   * Constructs a GenericRecord Provider.
   */
  public StringListToAvro(Provider<List<String>> provider, Schema recordSchema) {
    super(provider, recordSchema);
  }

  @Override
  protected Decoder makeDecoder(List<String> input) {
    return new StringListDecoder(input);
  }
}
