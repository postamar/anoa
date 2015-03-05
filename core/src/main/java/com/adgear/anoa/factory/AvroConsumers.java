package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.factory.util.AvroBatchWriteConsumer;
import com.adgear.anoa.factory.util.AvroWriteConsumer;
import com.adgear.anoa.factory.util.WriteConsumer;

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

public class AvroConsumers {

  static public @NonNull WriteConsumer<GenericRecord> batch(
      @NonNull File file,
      @NonNull Schema schema) {
    try {
      return new AvroBatchWriteConsumer<>(
          new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
              .create(schema, file));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> batch(
      @NonNull File file,
      @NonNull Class<R> recordClass) {
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

  static public @NonNull WriteConsumer<GenericRecord> batch(
      @NonNull OutputStream outputStream,
      @NonNull Schema schema) {
    try {
      return new AvroBatchWriteConsumer<>(
          new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
              .create(schema, outputStream));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> batch(
      @NonNull OutputStream outputStream,
      @NonNull Class<R> recordClass) {
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

  static public @NonNull WriteConsumer<GenericRecord> binary(
      @NonNull OutputStream outputStream,
      @NonNull Schema schema) {
    return new AvroWriteConsumer<>(
        new GenericDatumWriter<>(schema),
        EncoderFactory.get().binaryEncoder(new BufferedOutputStream(outputStream), null));
  }

  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> binary(
      @NonNull OutputStream outputStream,
      @NonNull Class<R> recordClass) {
    Schema schema = SpecificData.get().getSchema(recordClass);
    if (schema == null) {
      throw new IllegalArgumentException("No schema found for class " + recordClass);
    }
    return new AvroWriteConsumer<>(
        new SpecificDatumWriter<>(schema),
        EncoderFactory.get().binaryEncoder(new BufferedOutputStream(outputStream), null));
  }

  static public @NonNull WriteConsumer<GenericRecord> json(
      @NonNull OutputStream outputStream,
      @NonNull Schema schema) {
    try {
      return new AvroWriteConsumer<>(
          new GenericDatumWriter<>(schema),
          EncoderFactory.get().jsonEncoder(schema, new BufferedOutputStream(outputStream)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull WriteConsumer<R> json(
      @NonNull OutputStream outputStream,
      @NonNull Class<R> recordClass) {
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
}
