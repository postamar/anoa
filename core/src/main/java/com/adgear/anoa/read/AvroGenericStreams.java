package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;

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
import java.util.stream.Stream;


public class AvroGenericStreams {

  static public Stream<GenericRecord> binary(
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return binary(new GenericDatumReader<>(schema), inputStream);
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return binary(anoaFactory, new GenericDatumReader<>(schema), inputStream);
  }

  static public <R extends IndexedRecord> @NonNull Stream<R> binary(
      @NonNull GenericDatumReader<R> reader,
      @NonNull InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return ReadIteratorUtils.avro(reader, d, Unchecked.supplier(d::isEnd)).stream();
  }

  static public <R extends IndexedRecord, M> @NonNull Stream<Anoa<R, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull GenericDatumReader<R> reader,
      @NonNull InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return ReadIteratorUtils.avro(anoaFactory, reader, d, Unchecked.supplier(d::isEnd)).stream();
  }

  static public @NonNull Stream<GenericRecord> json(
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return json(new GenericDatumReader<>(schema), inputStream);
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return json(anoaFactory, new GenericDatumReader<>(schema), inputStream);
  }

  static public <R extends IndexedRecord> @NonNull Stream<R> json(
      @NonNull GenericDatumReader<R> reader,
      @NonNull InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getExpected(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return ReadIteratorUtils.avro(reader, decoder, () -> false).stream();
  }

  static public <R extends IndexedRecord, M> @NonNull Stream<Anoa<R, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull GenericDatumReader<R> reader,
      @NonNull InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getExpected(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return ReadIteratorUtils.avro(anoaFactory, reader, decoder, () -> false).stream();
  }

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull File file) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull File file) {
    try {
      return batch(anoaFactory, new DataFileReader<>(file, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public Stream<GenericRecord> batch(
      @Nullable Schema schema,
      @NonNull File file) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @Nullable Schema schema,
      @NonNull File file) {
    try {
      return batch(anoaFactory, new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream) {
    try {
      return batch(anoaFactory, new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public Stream<GenericRecord> batch(
      @Nullable Schema schema,
      @NonNull InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @Nullable Schema schema,
      @NonNull InputStream inputStream) {
    try {
      return batch(anoaFactory,
                   new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull DataFileStream<GenericRecord> dataFileStream) {
    return ReadIteratorUtils.avro(dataFileStream).stream();
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DataFileStream<GenericRecord> dataFileStream) {
    return ReadIteratorUtils.avro(anoaFactory, dataFileStream).stream();
  }

  static public Stream<GenericRecord> jackson(
      @NonNull Schema schema,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(jacksonParser).stream()
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(schema, strict));
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(anoaFactory, jacksonParser).stream()
        .map(anoaFactory.function(TreeNode::traverse))
        .map(AvroDecoders.jackson(anoaFactory, schema, strict));
  }
}
