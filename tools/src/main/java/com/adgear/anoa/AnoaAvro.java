package com.adgear.anoa;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AnoaAvro {


  static public DataFileWriter<GenericRecord> to(File file, Schema schema)
      throws IOException {
    return new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
        .create(schema, file);
  }

  static public <R extends SpecificRecord> DataFileWriter<R> to(File file, Class<R> recordClass)
      throws IOException {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new AvroRuntimeException("No schema found for class " + recordClass);
    }
    return new DataFileWriter<R>(new SpecificDatumWriter<>(schema)).create(schema, file);
  }

  static public DataFileWriter<GenericRecord> to(OutputStream outputStream, Schema schema)
      throws IOException {
    return new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
        .create(schema, outputStream);
  }

  static public <R extends SpecificRecord> DataFileWriter<R> to(OutputStream outputStream,
                                                                Class<R> recordClass)
      throws IOException {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new AvroRuntimeException("No schema found for class " + recordClass);
    }
    return new DataFileWriter<R>(new SpecificDatumWriter<>(schema)).create(schema, outputStream);
  }

  static public <R extends IndexedRecord> Stream<R> from(Iterable<R> iterable) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                                    iterable.iterator(),
                                    Spliterator.NONNULL | Spliterator.ORDERED),
                                false);
  }

  static public Stream<GenericRecord> from(File file, Schema schema) throws IOException {
    return from(DataFileReader.<GenericRecord>openReader(file, new GenericDatumReader<>(schema)));
  }

  static public Stream<GenericRecord> from(InputStream inputStream, Schema schema)
      throws IOException {
    return from(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
  }

  static public <R extends SpecificRecord> Stream<R> from(File file, Class<R> recordClass)
      throws IOException {
    return from(DataFileReader.openReader(file, new SpecificDatumReader<>(recordClass)));
  }

  static public <R extends SpecificRecord> Stream<R> from(InputStream inputStream,
                                                          Class<R> recordClass)
      throws IOException {
    return from(new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass)));
  }

  static public Stream<GenericRecord> fromGeneric(File file) throws IOException {
    return from(DataFileReader.<GenericRecord>openReader(file, new GenericDatumReader<>()));
  }

  static public Stream<GenericRecord> fromGeneric(InputStream inputStream) throws IOException {
    return from(new DataFileStream<>(inputStream, new GenericDatumReader<>()));
  }

  static public <R extends SpecificRecord> Stream<R> fromSpecific(File file) throws IOException {
    return from(DataFileReader.<R>openReader(file, new SpecificDatumReader<>()));
  }

  static public <R extends SpecificRecord> Stream<R> fromSpecific(InputStream inputStream)
      throws IOException {
    return from(new DataFileStream<>(inputStream, new SpecificDatumReader<>()));
  }
}
