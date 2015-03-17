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
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.stream.Stream;


public class AvroGenericStreams {

  static public @NonNull Stream<GenericRecord> binary(
      @NonNull InputStream inputStream,
      @NonNull Schema schema) {
    return binary(new GenericDatumReader<>(schema), inputStream);
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream,
      @NonNull Schema schema) {
    return binary(anoaFactory, new GenericDatumReader<>(schema), inputStream);
  }

  static <R extends IndexedRecord> Stream<R> binary(GenericDatumReader<R> reader,
                                                    InputStream inputStream) {
    return ReadIteratorUtils.avro(reader, DecoderFactory.get().binaryDecoder(inputStream, null))
        .stream();
  }

  static <R extends IndexedRecord, M> Stream<Anoa<R, M>> binary(AnoaFactory<M> anoaFactory,
                                                                GenericDatumReader<R> reader,
                                                                InputStream inputStream) {
    return ReadIteratorUtils.avro(anoaFactory,
                                  reader,
                                  DecoderFactory.get().binaryDecoder(inputStream, null))
        .stream();
  }

  static public @NonNull Stream<GenericRecord> json(
      @NonNull InputStream inputStream,
      @NonNull Schema schema) {
    return json(new GenericDatumReader<>(schema), inputStream);
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream,
      @NonNull Schema schema) {
    return json(anoaFactory, new GenericDatumReader<>(schema), inputStream);
  }

  static <R extends IndexedRecord> Stream<R> json(GenericDatumReader<R> reader,
                                                  InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getSchema(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return ReadIteratorUtils.avro(reader, decoder).stream();
  }

  static <R extends IndexedRecord, M> Stream<Anoa<R, M>> json(AnoaFactory<M> anoaFactory,
                                                              GenericDatumReader<R> reader,
                                                              InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getSchema(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return ReadIteratorUtils.avro(anoaFactory, reader, decoder).stream();
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

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull File file,
      @Nullable Schema schema) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull File file,
      @Nullable Schema schema) {
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

  static public @NonNull Stream<GenericRecord> batch(
      @NonNull InputStream inputStream,
      @Nullable Schema schema) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream,
      @Nullable Schema schema) {
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

  static public @NonNull Stream<GenericRecord> jackson(
      @NonNull JsonParser jacksonParser,
      @NonNull Schema schema,
      boolean strict) {
    return ReadIteratorUtils.jackson(jacksonParser).stream()
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(schema, strict));
  }

  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull JsonParser jacksonParser,
      @NonNull Schema schema,
      boolean strict) {
    return ReadIteratorUtils.jackson(anoaFactory, jacksonParser).stream()
        .map(anoaFactory.function(TreeNode::traverse))
        .map(AvroDecoders.jackson(anoaFactory, schema, strict));
  }
}
