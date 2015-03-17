package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

public class AvroSpecificStreams {

  static public <R extends SpecificRecord> @NonNull Stream<R> binary(
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    return AvroGenericStreams.binary(new SpecificDatumReader<>(recordClass), inputStream);
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    return AvroGenericStreams.binary(anoaFactory,
                                     new SpecificDatumReader<>(recordClass),
                                     inputStream);
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> json(
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    return AvroGenericStreams.json(new SpecificDatumReader<>(recordClass), inputStream);
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    return AvroGenericStreams.json(anoaFactory,
                                   new SpecificDatumReader<>(recordClass),
                                   inputStream);
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull File file) {
    try {
      return batch(new DataFileReader<>(file, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull File file) {
    try {
      return batch(anoaFactory, new DataFileReader<>(file, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull File file,
      @NonNull Class<R> recordClass) {
    try {
      return batch(new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull File file,
      @NonNull Class<R> recordClass) {
    try {
      return batch(anoaFactory, new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream) {
    try {
      return batch(anoaFactory, new DataFileStream<>(inputStream, new SpecificDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    try {
      return batch(new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream,
      @NonNull Class<R> recordClass) {
    try {
      return batch(anoaFactory,
                   new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull DataFileStream<R> dataFileStream) {
    return ReadIteratorUtils.avro(dataFileStream).stream();
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DataFileStream<R> dataFileStream) {
    return ReadIteratorUtils.avro(anoaFactory, dataFileStream).stream();
  }

  static public <R extends SpecificRecord> @NonNull Stream<R> jackson(
      @NonNull JsonParser jacksonParser,
      @NonNull Class<R> recordClass,
      boolean strict) {
    return ReadIteratorUtils.jackson(jacksonParser).stream()
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(recordClass, strict));
  }

  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull JsonParser jacksonParser,
      @NonNull Class<R> recordClass,
      boolean strict) {
    return ReadIteratorUtils.jackson(anoaFactory, jacksonParser).stream()
        .map(anoaFactory.function(TreeNode::traverse))
        .map(AvroDecoders.jackson(anoaFactory, recordClass, strict));
  }
}
