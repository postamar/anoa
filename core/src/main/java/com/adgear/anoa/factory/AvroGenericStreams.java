package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import com.adgear.anoa.factory.util.AvroReadIterator;
import com.adgear.anoa.factory.util.IteratorWrapper;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.stream.Stream;


public class AvroGenericStreams {

  static public @NonNull Stream<GenericRecord> binary(
      @NonNull InputStream inputStream,
      @NonNull Schema schema) {
    return from(new GenericDatumReader<>(schema),
                DecoderFactory.get().binaryDecoder(inputStream, null));
  }

  static public @NonNull Stream<GenericRecord> json(
      @NonNull InputStream inputStream,
      @NonNull Schema schema) {
    try {
      return from(new GenericDatumReader<>(schema),
                  DecoderFactory.get().jsonDecoder(schema, inputStream));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull File file) {
    try {
      return from(DataFileReader.<GenericRecord>openReader(file, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull File file,
      @Nullable Schema schema) {
    try {
      return from(DataFileReader.<GenericRecord>openReader(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull InputStream inputStream) {
    try {
      return from(new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull InputStream inputStream,
      @Nullable Schema schema) {
    try {
      return from(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> from(
      @NonNull Iterator<GenericRecord> iterator) {
    return new IteratorWrapper<>(iterator).stream();
  }

  static public @NonNull Stream<GenericRecord> from(
      @NonNull DatumReader<GenericRecord> reader,
      @NonNull Decoder decoder) {
    return new AvroReadIterator<>(reader, decoder).stream();
  }
}
