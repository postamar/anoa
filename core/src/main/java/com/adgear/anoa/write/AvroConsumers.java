package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

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
public class AvroConsumers {

  /**
   * Write as compressed Avro batch file, readable with {@link org.apache.avro.file.DataFileStream}
   *
   * @param schema Avro record schema to accept
   * @param file file to write into
   */
  static public @NonNull WriteConsumer<GenericRecord> batch(
      @NonNull Schema schema,
      @NonNull File file) {
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
   * @param file file to write into
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> batch(
      @NonNull Class<R> recordClass,
      @NonNull File file) {
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
   * @param schema Avro record schema to accept
   * @param outputStream stream to write into
   */
  static public @NonNull WriteConsumer<GenericRecord> batch(
      @NonNull Schema schema,
      @NonNull OutputStream outputStream) {
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
   * @param recordClass Avro SpecificRecord class to accept
   * @param outputStream stream to write into
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> batch(
      @NonNull Class<R> recordClass,
      @NonNull OutputStream outputStream) {
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
   * @param schema Avro schema to accept
   * @param outputStream stream to write into
   */
  static public @NonNull WriteConsumer<GenericRecord> binary(
      @NonNull Schema schema,
      @NonNull OutputStream outputStream) {
    return new AvroWriteConsumer<>(
        new GenericDatumWriter<>(schema),
        EncoderFactory.get().binaryEncoder(new BufferedOutputStream(outputStream), null));
  }

  /**
   * Write as Avro binary encoding
   *
   * @param recordClass Avro SpecificRecord class to accept
   * @param outputStream stream to write into
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> binary(
      @NonNull Class<R> recordClass,
      @NonNull OutputStream outputStream) {
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
   * @param schema Avro schema to accept
   * @param outputStream stream to write into
   */
  static public @NonNull WriteConsumer<GenericRecord> json(
      @NonNull Schema schema,
      @NonNull OutputStream outputStream) {
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
   * @param recordClass Avro SpecificRecord class to accept
   * @param outputStream stream to write into
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> json(
      @NonNull Class<R> recordClass,
      @NonNull OutputStream outputStream) {
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
   * Write as 'natural' JSON serialization using provided generator
   *
   * @param schema Avro schema to accept
   * @param jacksonGenerator JsonGenerator instance to write into
   */
  static public @NonNull WriteConsumer<GenericRecord> jackson(
      @NonNull Schema schema,
      @NonNull JsonGenerator jacksonGenerator) {
    return new JacksonWriteConsumer<>(jacksonGenerator, new AvroWriter<GenericRecord>(schema));
  }

  /**
   * Write as 'natural' JSON serialization using provided generator
   *
   * @param recordClass Avro SpecificRecord class to accept
   * @param jacksonGenerator JsonGenerator instance to write into
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord>
  @NonNull WriteConsumer<R> jackson(
      @NonNull Class<R> recordClass,
      @NonNull JsonGenerator jacksonGenerator) {
    return new JacksonWriteConsumer<>(jacksonGenerator, new AvroWriter<>(recordClass));
  }
}
