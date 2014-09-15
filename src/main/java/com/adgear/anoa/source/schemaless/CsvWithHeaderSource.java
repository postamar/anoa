package com.adgear.anoa.source.schemaless;

import com.adgear.anoa.source.avro.AvroSource;

import org.apache.avro.Schema;

import java.io.Reader;
import java.util.List;

/**
 * Iterates over comma-separated values expecting a column header row. Records exposed as String
 * lists.
 *
 * @see com.adgear.anoa.source.schemaless.CsvWithHeaderSource
 * @see com.adgear.anoa.source.avro.AvroSource
 */
public class CsvWithHeaderSource extends CsvSource implements AvroSource<List<String>> {

  public CsvWithHeaderSource(Reader in) {
    super(in, true);
  }

  /**
   * @return an Avro Schema induced from the column header.
   */
  @Override
  public Schema getAvroSchema() {
    return avroSchema;
  }
}
