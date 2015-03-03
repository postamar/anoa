package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.factory.util.AvroReadIterator;
import com.adgear.anoa.factory.util.IteratorWrapper;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class AvroSpecificStreams {

  static public <R extends SpecificRecord> @NonNull Stream<R> binary(
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    return from(new SpecificDatumReader<>(recordClass),
                DecoderFactory.get().binaryDecoder(inputStream, null));
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> json(
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    SpecificDatumReader<R> reader = new SpecificDatumReader<>(recordClass);
    try {
      return from(reader, DecoderFactory.get().jsonDecoder(reader.getSchema(), inputStream));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull File file) {
    try {
      return from(DataFileReader.<R>openReader(file, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull File file,
      @NonNull Class<R> recordClass) {
    try {
      return from(DataFileReader.openReader(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull InputStream inputStream) {
    try {
      return from(new DataFileStream<>(inputStream, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    try {
      return from(new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> Stream<R> from(
      Iterator<R> iterator) {
    return new IteratorWrapper<>(iterator).stream();
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> from(
      @NonNull DatumReader<R> reader,
      @NonNull Decoder decoder) {
    return new AvroReadIterator<R>(reader, decoder).stream();
  }

}
