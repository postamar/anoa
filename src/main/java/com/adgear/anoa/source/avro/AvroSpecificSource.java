package com.adgear.anoa.source.avro;

import com.adgear.anoa.provider.base.CounterlessProviderBase;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Iterates over SpecificRecords in an Avro data file.
 *
 * @param <R> Type of the records to be provided.
 * @see com.adgear.anoa.source.avro.AvroSource
 */
public class AvroSpecificSource<R extends IndexedRecord>
    extends CounterlessProviderBase<R> implements AvroSource<R> {

  final private Schema schema;
  final private DataFileStream<R> stream;

  protected AvroSpecificSource(InputStream in) throws IOException {
    stream = new DataFileStream<>(in, new GenericDatumReader<R>());
    schema = stream.getSchema();
  }

  /**
   * @param in          An input stream to an Avro Data File, which provides 'writer' schema.
   * @param recordClass The class object describing the provided SpecificRecords, and from which the
   *                    'reader' Schema is inferred
   */
  public AvroSpecificSource(InputStream in, Class<R> recordClass) throws IOException {
    stream = new DataFileStream<>(in, new SpecificDatumReader<>(recordClass));
    schema = SpecificData.get().getSchema(recordClass);
  }

  /**
   * @param in           An input stream to an Avro Data File, which provides 'writer' schema.
   * @param recordSchema The 'reader' Schema, from which the correct SpecificRecord implementation
   *                     is inferred.
   */
  public AvroSpecificSource(InputStream in, Schema recordSchema) throws IOException {
    stream = new DataFileStream<>(in, new GenericDatumReader<R>(recordSchema));
    schema = recordSchema;
  }

  @Override
  public Schema getAvroSchema() {
    return schema;
  }

  @Override
  protected R getNext() throws IOException {
    return stream.next();
  }

  @Override
  public boolean hasNext() {
    return stream.hasNext();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
