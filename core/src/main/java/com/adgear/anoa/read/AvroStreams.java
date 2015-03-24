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
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.jooq.lambda.Unchecked;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

/**
 * Utility class for deserializing Avro {@link org.apache.avro.generic.GenericRecord} or
 * {@link org.apache.avro.specific.SpecificRecord} instances as a {@link java.util.stream.Stream}.
 */
public class AvroStreams {

  /**
   * @param schema Avro record schema
   * @param inputStream data source
   */
  static public Stream<GenericRecord> binary(
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return binary(new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro record schema
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return binary(anoaFactory, new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   */
  static public Stream<GenericRecord> binary(
      @NonNull Schema writer,
      @NonNull Schema reader,
      @NonNull InputStream inputStream) {
    return binary(new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema writer,
      @NonNull Schema reader,
      @NonNull InputStream inputStream) {
    return binary(anoaFactory, new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> @NonNull Stream<R> binary(
      @NonNull Class<R> recordClass,
      @NonNull InputStream inputStream) {
    return binary(new SpecificDatumReader<>(recordClass), inputStream);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass,
      @NonNull InputStream inputStream) {
    return binary(anoaFactory, new SpecificDatumReader<>(recordClass), inputStream);
  }

  static <R extends IndexedRecord> @NonNull Stream<R> binary(
      @NonNull GenericDatumReader<R> reader,
      @NonNull InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return ReadIteratorUtils.avro(reader, d, Unchecked.supplier(d::isEnd)).stream();
  }

  static <R extends IndexedRecord, M> @NonNull Stream<Anoa<R, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull GenericDatumReader<R> reader,
      @NonNull InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return ReadIteratorUtils.avro(anoaFactory, reader, d, Unchecked.supplier(d::isEnd)).stream();
  }

  /**
   * @param schema Avro record schema
   * @param inputStream data source
   */
  static public @NonNull Stream<GenericRecord> json(
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return json(new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro record schema
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      @NonNull InputStream inputStream) {
    return json(anoaFactory, new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   */
  static public @NonNull Stream<GenericRecord> json(
      @NonNull Schema writer,
      @NonNull Schema reader,
      @NonNull InputStream inputStream) {
    return json(new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema writer,
      @NonNull Schema reader,
      @NonNull InputStream inputStream) {
    return json(anoaFactory, new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> @NonNull Stream<R> json(
      @NonNull Class<R> recordClass,
      @NonNull InputStream inputStream) {
    return json(new SpecificDatumReader<>(recordClass), inputStream);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass,
      @NonNull InputStream inputStream) {
    return json(anoaFactory, new SpecificDatumReader<>(recordClass), inputStream);
  }

  static <R extends IndexedRecord> @NonNull Stream<R> json(
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

  static <R extends IndexedRecord, M> @NonNull Stream<Anoa<R, M>> json(
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

  /**
   * @param inputStream data source
   */
  static public @NonNull Stream<GenericRecord> batch(
      @NonNull InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull InputStream inputStream) {
    try {
      return batch(anoaFactory, new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param file data source
   */
  static public @NonNull Stream<GenericRecord> batch(
      @NonNull File file) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<GenericRecord>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param file data source
   * @param <M> Metadata type
   */
  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull File file) {
    try {
      return batch(anoaFactory, new DataFileReader<>(file, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param schema Avro record schema
   * @param inputStream data source
   */
  static public Stream<GenericRecord> batch(
      @Nullable Schema schema,
      @NonNull InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro record schema
   * @param inputStream data source
   * @param <M> Metadata type
   */
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

  /**
   * @param schema Avro record schema
   * @param file data source
   */
  static public Stream<GenericRecord> batch(
      @Nullable Schema schema,
      @NonNull File file) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro record schema
   * @param file data source
   * @param <M> Metadata type
   */
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

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull Class<R> recordClass,
      @NonNull InputStream inputStream) {
    final DataFileStream<R> dataFileStream;
    try {
      dataFileStream = new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return batch(dataFileStream);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass,
      @NonNull InputStream inputStream) {
    final DataFileStream<R> dataFileStream;
    try {
      dataFileStream = new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return batch(anoaFactory, dataFileStream);
  }


  /**
   * @param recordClass Avro SpecificRecord class object
   * @param file data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> @NonNull Stream<R> batch(
      @NonNull Class<R> recordClass,
      @NonNull File file) {
    try {
      return batch(new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param file data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass,
      @NonNull File file) {
    try {
      return batch(anoaFactory, new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param dataFileStream data source
   * @param <R> Avro record type
   */
  static public <R extends IndexedRecord> @NonNull Stream<R> batch(
      @NonNull DataFileStream<R> dataFileStream) {
    return ReadIteratorUtils.avro(dataFileStream).stream();
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param dataFileStream data source
   * @param <R> Avro record type
   * @param <M> Metadata type
   */
  static public <R extends IndexedRecord, M> @NonNull Stream<Anoa<R, M>> batch(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DataFileStream<R> dataFileStream) {
    return ReadIteratorUtils.avro(anoaFactory, dataFileStream).stream();
  }

  /**
   * @param schema Avro record schema
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   */
  static public Stream<GenericRecord> jackson(
      @NonNull Schema schema,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(jacksonParser).stream()
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(schema, strict));
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro record schema
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <M> Metadata type
   */
  static public <M> @NonNull Stream<Anoa<GenericRecord, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(anoaFactory, jacksonParser).stream()
        .map(anoaFactory.function(TreeNode::traverse))
        .map(AvroDecoders.jackson(anoaFactory, schema, strict));
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> @NonNull Stream<R> jackson(
      @NonNull Class<R> recordClass,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(jacksonParser).stream()
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(recordClass, strict));
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> @NonNull Stream<Anoa<R, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass,
      boolean strict,
      @NonNull JsonParser jacksonParser) {
    return ReadIteratorUtils.jackson(anoaFactory, jacksonParser).stream()
        .map(anoaFactory.function(TreeNode::traverse))
        .map(AvroDecoders.jackson(anoaFactory, recordClass, strict));
  }
}
