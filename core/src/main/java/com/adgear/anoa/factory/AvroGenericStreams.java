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
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.jooq.lambda.Unchecked;

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
    return binary(new GenericDatumReader<>(schema), inputStream);
  }

  static <R extends IndexedRecord> Stream<R> binary(GenericDatumReader<R> reader,
                                                    InputStream inputStream) {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    return new AvroReadIterator<>(reader, decoder, Unchecked.supplier(decoder::isEnd)).stream();
  }

  static public @NonNull Stream<GenericRecord> json(
      @NonNull InputStream inputStream,
      @NonNull Schema schema) {
    return json(new GenericDatumReader<>(schema), inputStream);
  }

  static <R extends IndexedRecord> Stream<R> json(GenericDatumReader<R> reader,
                                                  InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getSchema(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new AvroReadIterator<>(reader, decoder, () -> false).stream();
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
}
