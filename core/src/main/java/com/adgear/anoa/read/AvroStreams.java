package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonParser;

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
 * Utility class for deserializing Avro {@link org.apache.avro.generic.GenericRecord} or {@link
 * org.apache.avro.specific.SpecificRecord} instances as a {@link java.util.stream.Stream}.
 */
public class AvroStreams {

  protected AvroStreams() {
  }

  /**
   * @param schema      Avro record schema
   * @param inputStream data source
   */
  static public Stream<GenericRecord> binary(
      Schema schema,
      InputStream inputStream) {
    return binary(new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param inputStream data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> binary(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      InputStream inputStream) {
    return binary(anoaHandler, new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param inputStream data source
   */
  static public Stream<GenericRecord> binary(
      Schema writer,
      Schema reader,
      InputStream inputStream) {
    return binary(new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param inputStream data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> binary(
      AnoaHandler<M> anoaHandler,
      Schema writer,
      Schema reader,
      InputStream inputStream) {
    return binary(anoaHandler, new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R>         Avro SpecificData record type
   */
  static public <R extends SpecificRecord> Stream<R> binary(
      Class<R> recordClass,
      InputStream inputStream) {
    return binary(new SpecificDatumReader<>(recordClass), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R>         Avro SpecificData record type
   * @param <M>         Metadata type
   */
  static public <R extends SpecificRecord, M> Stream<Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      InputStream inputStream) {
    return binary(anoaHandler, new SpecificDatumReader<>(recordClass), inputStream);
  }

  static <R extends IndexedRecord> Stream<R> binary(
      GenericDatumReader<R> reader,
      InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return LookAheadIteratorFactory
        .avro(reader, d, Unchecked.supplier(d::isEnd), inputStream).asStream();
  }

  static <R extends IndexedRecord, M> Stream<Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      GenericDatumReader<R> reader,
      InputStream inputStream) {
    final BinaryDecoder d = DecoderFactory.get().binaryDecoder(inputStream, null);
    return LookAheadIteratorFactory
        .avro(anoaHandler, reader, d, Unchecked.supplier(d::isEnd), inputStream).asStream();
  }

  /**
   * @param schema      Avro record schema
   * @param inputStream data source
   */
  static public Stream<GenericRecord> json(
      Schema schema,
      InputStream inputStream) {
    return json(new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param inputStream data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> json(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      InputStream inputStream) {
    return json(anoaHandler, new GenericDatumReader<>(schema), inputStream);
  }

  /**
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param inputStream data source
   */
  static public Stream<GenericRecord> json(
      Schema writer,
      Schema reader,
      InputStream inputStream) {
    return json(new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param inputStream data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> json(
      AnoaHandler<M> anoaHandler,
      Schema writer,
      Schema reader,
      InputStream inputStream) {
    return json(anoaHandler, new GenericDatumReader<>(writer, reader), inputStream);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R>         Avro SpecificData record type
   */
  static public <R extends SpecificRecord> Stream<R> json(
      Class<R> recordClass,
      InputStream inputStream) {
    return json(new SpecificDatumReader<>(recordClass), inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R>         Avro SpecificData record type
   * @param <M>         Metadata type
   */
  static public <R extends SpecificRecord, M> Stream<Anoa<R, M>> json(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      InputStream inputStream) {
    return json(anoaHandler, new SpecificDatumReader<>(recordClass), inputStream);
  }

  static <R extends IndexedRecord> Stream<R> json(
      GenericDatumReader<R> reader,
      InputStream inputStream) {
    final JsonDecoder decoder;
    try {
      decoder = DecoderFactory.get().jsonDecoder(reader.getExpected(), inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return LookAheadIteratorFactory.avro(reader, decoder, () -> false, inputStream).asStream();
  }

  static <R extends IndexedRecord, M> Stream<Anoa<R, M>> json(
      AnoaHandler<M> anoaHandler,
      GenericDatumReader<R> reader,
      InputStream inputStream) {
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
   * @param schema      Avro record schema
   * @param inputStream data source
   */
  static public Stream<GenericRecord> batch(
      Schema schema,
      InputStream inputStream) {
    try {
      return batch(new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param inputStream data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> batch(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      InputStream inputStream) {
    try {
      return batch(anoaHandler,
                   new DataFileStream<>(inputStream, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param schema Avro record schema
   * @param file   data source
   */
  static public Stream<GenericRecord> batch(
      Schema schema,
      File file) {
    try {
      return batch(new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param file        data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> batch(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      File file) {
    try {
      return batch(anoaHandler, new DataFileReader<>(file, new GenericDatumReader<>(schema)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param inputStream data source
   */
  static public Stream<GenericRecord> batch(
      InputStream inputStream) {
    return batch((Schema) null, inputStream);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param inputStream data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> batch(
      AnoaHandler<M> anoaHandler,
      InputStream inputStream) {
    return batch(anoaHandler, (Schema) null, inputStream);
  }

  /**
   * @param file data source
   */
  static public Stream<GenericRecord> batch(
      File file) {
    return batch((Schema) null, file);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param file        data source
   * @param <M>         Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> batch(
      AnoaHandler<M> anoaHandler,
      File file) {
    return batch(anoaHandler, (Schema) null, file);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param inputStream data source
   * @param <R>         Avro SpecificData record type
   */
  static public <R extends SpecificRecord> Stream<R> batch(
      Class<R> recordClass,
      InputStream inputStream) {
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
   * @param <R>         Avro SpecificData record type
   * @param <M>         Metadata type
   */
  static public <R extends SpecificRecord, M> Stream<Anoa<R, M>> batch(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      InputStream inputStream) {
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
   * @param file        data source
   * @param <R>         Avro SpecificData record type
   */
  static public <R extends SpecificRecord> Stream<R> batch(
      Class<R> recordClass,
      File file) {
    try {
      return batch(new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param file        data source
   * @param <R>         Avro SpecificData record type
   * @param <M>         Metadata type
   */
  static public <R extends SpecificRecord, M> Stream<Anoa<R, M>> batch(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      File file) {
    try {
      return batch(anoaHandler, new DataFileReader<>(file, new SpecificDatumReader<>(recordClass)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * @param dataFileStream data source
   * @param <R>            Avro record type
   */
  static public <R extends IndexedRecord> Stream<R> batch(
      DataFileStream<R> dataFileStream) {
    return LookAheadIteratorFactory.avro(dataFileStream).asStream();
  }

  /**
   * @param anoaHandler    {@code AnoaHandler} instance to use for exception handling
   * @param dataFileStream data source
   * @param <R>            Avro record type
   * @param <M>            Metadata type
   */
  static public <R extends IndexedRecord, M> Stream<Anoa<R, M>> batch(
      AnoaHandler<M> anoaHandler,
      DataFileStream<R> dataFileStream) {
    return LookAheadIteratorFactory.avro(anoaHandler, dataFileStream).asStream();
  }

  /**
   * @param schema        Avro record schema
   * @param strict        enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   */
  static public Stream<GenericRecord> jackson(
      Schema schema,
      boolean strict,
      JsonParser jacksonParser) {
    return JacksonUtils.stream(new AvroReader.GenericReader(schema), strict, jacksonParser);
  }

  /**
   * @param anoaHandler   {@code AnoaHandler} instance to use for exception handling
   * @param schema        Avro record schema
   * @param strict        enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <M>           Metadata type
   */
  static public <M> Stream<Anoa<GenericRecord, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      boolean strict,
      JsonParser jacksonParser) {
    return JacksonUtils.stream(anoaHandler,
                               new AvroReader.GenericReader(schema),
                               strict,
                               jacksonParser);
  }

  /**
   * @param recordClass   Avro SpecificRecord class object
   * @param strict        enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Avro SpecificData record type
   */
  static public <R extends SpecificRecord> Stream<R> jackson(
      Class<R> recordClass,
      boolean strict,
      JsonParser jacksonParser) {
    return JacksonUtils.stream(new AvroReader.SpecificReader<>(recordClass),
                               strict,
                               jacksonParser);
  }

  /**
   * @param anoaHandler   {@code AnoaHandler} instance to use for exception handling
   * @param recordClass   Avro SpecificRecord class object
   * @param strict        enable strict type checking
   * @param jacksonParser JsonParser instance from which to read
   * @param <R>           Avro SpecificData record type
   * @param <M>           Metadata type
   */
  static public <R extends SpecificRecord, M> Stream<Anoa<R, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      boolean strict,
      JsonParser jacksonParser) {
    return JacksonUtils.stream(anoaHandler,
                               new AvroReader.SpecificReader<>(recordClass),
                               strict,
                               jacksonParser);
  }
}
