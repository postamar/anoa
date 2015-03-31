package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
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

  protected AvroStreams() {
  }

  /**
   * @param schema Avro record schema
   * @param inputStream data source
   */
  static public Stream<GenericRecord> binary(
      /*@NonNull*/ Schema schema,
      /*@NonNull*/ InputStream inputStream) {
    return binary(new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema schema,
      /*@NonNull*/ InputStream inputStream) {
    return binary(anoaHandler, new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   */
  static public Stream<GenericRecord> binary(
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@NonNull*/ InputStream inputStream) {
    return binary(new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@NonNull*/ InputStream inputStream) {
    return binary(anoaHandler, new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> /*@NonNull*/ Stream<R> binary(
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ InputStream inputStream) {
    return binary(new SpecificDatumReader<>(recordClass), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ InputStream inputStream) {
    return binary(anoaHandler, new SpecificDatumReader<>(recordClass), inputStream);
  }

  static <R extends IndexedRecord> /*@NonNull*/ Stream<R> binary(
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return LookAheadIteratorFactory
        .avro(reader, d, Unchecked.supplier(d::isEnd), inputStream).asStream();
  }

  static <R extends IndexedRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return LookAheadIteratorFactory
        .avro(anoaHandler, reader, d, Unchecked.supplier(d::isEnd), inputStream).asStream();
  }

  /**
   * @param schema Avro record schema
   * @param inputStream data source
   */
  static public /*@NonNull*/ Stream<GenericRecord> json(
      /*@NonNull*/ Schema schema,
      /*@NonNull*/ InputStream inputStream) {
    return json(new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema schema,
      /*@NonNull*/ InputStream inputStream) {
    return json(anoaHandler, new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   */
  static public /*@NonNull*/ Stream<GenericRecord> json(
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@NonNull*/ InputStream inputStream) {
    return json(new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer Avro schema with which the record was originally serialized 
   * @param reader Avro schema to use for deserialization
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@NonNull*/ InputStream inputStream) {
    return json(anoaHandler, new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> /*@NonNull*/ Stream<R> json(
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ InputStream inputStream) {
    return json(new SpecificDatumReader<>(recordClass), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ InputStream inputStream) {
    return json(anoaHandler, new SpecificDatumReader<>(recordClass), inputStream);
  }

  static <R extends IndexedRecord> /*@NonNull*/ Stream<R> json(
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getExpected(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return LookAheadIteratorFactory.avro(reader, decoder, () -> false, inputStream).asStream();
  }

  static <R extends IndexedRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getExpected(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return LookAheadIteratorFactory
        .avro(anoaHandler, reader, decoder, () -> false, inputStream).asStream();
  }

  /**
   * @param inputStream data source
   */
  static public /*@NonNull*/ Stream<GenericRecord> batch(
      /*@NonNull*/ InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> batch(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ InputStream inputStream) {
    try {
      return batch(anoaHandler, new DataFileStream<>(inputStream, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param file data source
   */
  static public /*@NonNull*/ Stream<GenericRecord> batch(
      /*@NonNull*/ File file) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<GenericRecord>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param file data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> batch(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ File file) {
    try {
      return batch(anoaHandler, new DataFileReader<>(file, new GenericDatumReader<>()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param schema Avro record schema
   * @param inputStream data source
   */
  static public Stream<GenericRecord> batch(
      /*@Nullable*/ Schema schema,
      /*@NonNull*/ InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param inputStream data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> batch(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@Nullable*/ Schema schema,
      /*@NonNull*/ InputStream inputStream) {
    try {
      return batch(anoaHandler,
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
      /*@Nullable*/ Schema schema,
      /*@NonNull*/ File file) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param file data source
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> batch(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@Nullable*/ Schema schema,
      /*@NonNull*/ File file) {
    try {
      return batch(anoaHandler, new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> /*@NonNull*/ Stream<R> batch(
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ InputStream inputStream) {
    final DataFileStream<R> dataFileStream;
    try {
      dataFileStream = new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return batch(dataFileStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> batch(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ InputStream inputStream) {
    final DataFileStream<R> dataFileStream;
    try {
      dataFileStream = new DataFileStream<>(inputStream, new SpecificDatumReader<>(recordClass));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return batch(anoaHandler, dataFileStream);
  }


  /**
   * @param recordClass Avro SpecificRecord class object
   * @param file data source
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> /*@NonNull*/ Stream<R> batch(
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ File file) {
    try {
      return batch(new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param file data source
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> batch(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      /*@NonNull*/ File file) {
    try {
      return batch(anoaHandler, new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param dataFileStream data source
   * @param <R> Avro record type
   */
  static public <R extends IndexedRecord> /*@NonNull*/ Stream<R> batch(
      /*@NonNull*/ DataFileStream<R> dataFileStream) {
    return LookAheadIteratorFactory.avro(dataFileStream).asStream();
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param dataFileStream data source
   * @param <R> Avro record type
   * @param <M> Metadata type
   */
  static public <R extends IndexedRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> batch(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ DataFileStream<R> dataFileStream) {
    return LookAheadIteratorFactory.avro(anoaHandler, dataFileStream).asStream();
  }

  /**
   * @param schema Avro record schema
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   */
  static public Stream<GenericRecord> jackson(
      /*@NonNull*/ Schema schema,
      boolean strict,
      /*@NonNull*/ JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(jacksonParser).asStream()
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(schema, strict));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <M> Metadata type
   */
  static public <M> /*@NonNull*/ Stream<Anoa<GenericRecord, M>> jackson(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema schema,
      boolean strict,
      /*@NonNull*/ JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(anoaHandler, jacksonParser).asStream()
        .map(anoaHandler.function(TreeNode::traverse))
        .map(AvroDecoders.jackson(anoaHandler, schema, strict));
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R> Avro SpecificData record type
   */
  static public <R extends SpecificRecord> /*@NonNull*/ Stream<R> jackson(
      /*@NonNull*/ Class<R> recordClass,
      boolean strict,
      /*@NonNull*/ JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(jacksonParser).asStream()
        .map(TreeNode::traverse)
        .map(AvroDecoders.jackson(recordClass, strict));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param strict enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R> Avro SpecificData record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> /*@NonNull*/ Stream<Anoa<R, M>> jackson(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      boolean strict,
      /*@NonNull*/ JsonParser jacksonParser) {
    return LookAheadIteratorFactory.jackson(anoaHandler, jacksonParser).asStream()
        .map(anoaHandler.function(TreeNode::traverse))
        .map(AvroDecoders.jackson(anoaHandler, recordClass, strict));
  }
}
