package com.adgear.anoa.write;

import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * Utility class for generating {@code WriteConsumer} instances to write Avro records.
 */
final public class AvroConsumers {

  private AvroConsumers() {
  }

  /**
   * Write as compressed Avro batch file, readable with {@link org.apache.avro.file.DataFileStream}
   *
   * @param schema Avro record schema to accept
   * @param file   file to write into
   */
  static public WriteConsumer<GenericRecord> batch(
      Schema schema,
      File file) {
    try {
      return new AvroBatchWriteConsumer<>(
          new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
              .create(schema, file));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as compressed Avro batch file, readable with {@link org.apache.avro.file.DataFileStream}
   *
   * @param recordClass Avro SpecificRecord class to accept
   * @param file        file to write into
   * @param <R>         Avro record type
   */
  static public <R extends SpecificRecord> WriteConsumer<R> batch(
      Class<R> recordClass,
      File file) {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new IllegalArgumentException("No schema found for class " + recordClass);
    }
    try {
      return new AvroBatchWriteConsumer<>(
          new DataFileWriter<R>(new SpecificDatumWriter<>(schema)).create(schema, file));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as compressed Avro batch file, readable with {@link org.apache.avro.file.DataFileStream}
   *
   * @param schema       Avro record schema to accept
   * @param outputStream stream to write into
   */
  static public WriteConsumer<GenericRecord> batch(
      Schema schema,
      OutputStream outputStream) {
    try {
      return new AvroBatchWriteConsumer<>(
          new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
              .create(schema, outputStream));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as compressed Avro batch file, readable with {@link org.apache.avro.file.DataFileStream}
   *
   * @param recordClass  Avro SpecificRecord class to accept
   * @param outputStream stream to write into
   * @param <R>          Avro record type
   */
  static public <R extends SpecificRecord> WriteConsumer<R> batch(
      Class<R> recordClass,
      OutputStream outputStream) {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new IllegalArgumentException("No schema found for class " + recordClass);
    }
    try {
      return new AvroBatchWriteConsumer<>(
          new DataFileWriter<R>(new SpecificDatumWriter<>(schema)).create(schema, outputStream));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as Avro binary encoding
   *
   * @param schema       Avro schema to accept
   * @param outputStream stream to write into
   */
  static public WriteConsumer<GenericRecord> binary(
      Schema schema,
      OutputStream outputStream) {
    return new AvroWriteConsumer<>(
        new GenericDatumWriter<>(schema),
        EncoderFactory.get().binaryEncoder(new BufferedOutputStream(outputStream), null));
  }

  /**
   * Write as Avro binary encoding
   *
   * @param recordClass  Avro SpecificRecord class to accept
   * @param outputStream stream to write into
   * @param <R>          Avro record type
   */
  static public <R extends SpecificRecord> WriteConsumer<R> binary(
      Class<R> recordClass,
      OutputStream outputStream) {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new IllegalArgumentException("No schema found for class " + recordClass);
    }
    return new AvroWriteConsumer<>(
        new SpecificDatumWriter<>(schema),
        EncoderFactory.get().binaryEncoder(new BufferedOutputStream(outputStream), null));
  }

  /**
   * Write as Avro JSON encoding
   *
   * @param schema       Avro schema to accept
   * @param outputStream stream to write into
   */
  static public WriteConsumer<GenericRecord> json(
      Schema schema,
      OutputStream outputStream) {
    try {
      return new AvroWriteConsumer<>(
          new GenericDatumWriter<>(schema),
          EncoderFactory.get().jsonEncoder(schema, new BufferedOutputStream(outputStream)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as Avro JSON encoding
   *
   * @param recordClass  Avro SpecificRecord class to accept
   * @param outputStream stream to write into
   * @param <R>          Avro record type
   */
  static public <R extends SpecificRecord> WriteConsumer<R> json(
      Class<R> recordClass,
      OutputStream outputStream) {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new IllegalArgumentException("No schema found for class " + recordClass);
    }
    try {
      return new AvroWriteConsumer<>(
          new SpecificDatumWriter<>(schema),
          EncoderFactory.get().jsonEncoder(schema, new BufferedOutputStream(outputStream)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Write as 'natural' compact JSON serialization using provided generator.
   *
   * @param schema           Avro schema to accept
   * @param jacksonGenerator JsonGenerator instance to write into
   */
  static public WriteConsumer<GenericRecord> jackson(
      Schema schema,
      JsonGenerator jacksonGenerator) {
    return new AvroWriter<GenericRecord>(schema).writeConsumer(jacksonGenerator);
  }

  /**
   * Write as 'natural' strict JSON serialization using provided generator.
   *
   * @param schema           Avro schema to accept
   * @param jacksonGenerator JsonGenerator instance to write into
   */
  static public WriteConsumer<GenericRecord> jacksonStrict(
      Schema schema,
      JsonGenerator jacksonGenerator) {
    return new AvroWriter<GenericRecord>(schema).writeConsumerStrict(jacksonGenerator);
  }

  /**
   * Write as 'natural' compact JSON serialization using provided generator
   *
   * @param recordClass      Avro SpecificRecord class to accept
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <R>              Avro record type
   */
  static public <R extends SpecificRecord>
  WriteConsumer<R> jackson(
      Class<R> recordClass,
      JsonGenerator jacksonGenerator) {
    return new AvroWriter<>(recordClass).writeConsumer(jacksonGenerator);
  }

  /**
   * Write as 'natural' strict JSON serialization using provided generator
   *
   * @param recordClass      Avro SpecificRecord class to accept
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <R>              Avro record type
   */
  static public <R extends SpecificRecord>
  WriteConsumer<R> jacksonStrict(
      Class<R> recordClass,
      JsonGenerator jacksonGenerator) {
    return new AvroWriter<>(recordClass).writeConsumerStrict(jacksonGenerator);
  }
}
