package com.adgear.anoa.source.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;

/**
 * Iterates over GenericRecords in an Avro data file.
 *
 * @see com.adgear.anoa.source.avro.AvroSource
 */
public class AvroGenericSource extends AvroSpecificSource<GenericRecord> {

  /**
   * @param in An input stream to an Avro Data File. GenericRecord Schema is inferred from file
   *           header.
   */
  public AvroGenericSource(InputStream in) throws IOException {
    super(in);
  }

  /**
   * @param in           An input stream to an Avro Data File, which provides 'writer' Schema.
   * @param recordSchema The 'reader' Schema, which describes the provided GenericRecords.
   */
  public AvroGenericSource(InputStream in, Schema recordSchema) throws IOException {
    super(in, recordSchema);
  }
}
