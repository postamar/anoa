package com.adgear.anoa.source.schemaless;

import com.adgear.anoa.source.avro.AvroSource;

import org.apache.avro.Schema;

import java.io.Reader;
import java.util.List;


/**
 * Iterates over tab-separated values expecting a column header row. Records exposed as String
 * lists.
 *
 * @see com.adgear.anoa.source.schemaless.TsvWithHeaderSource
 * @see com.adgear.anoa.source.avro.AvroSource
 */
public class TsvWithHeaderSource extends TsvSource implements AvroSource<List<String>> {

  public TsvWithHeaderSource(Reader in) {
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
