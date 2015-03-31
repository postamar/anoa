package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility class for generating functions for deserializing Avro records. Unless specified
 * otherwise, the functions should not be deemed thread-safe.
 */
public class AvroDecoders {

  protected AvroDecoders() {
  }

  static protected <R extends IndexedRecord> Supplier<R> ofNullable(Supplier<R> supplier) {
    return (supplier != null) ? supplier : () -> ((R) null);
  }

  /**
   * @param schema Avro record schema
   * @param supplier when not null, used for record instantiation
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public /*@NonNull*/ Function<byte[], GenericRecord> binary(
      /*@NonNull*/ Schema schema,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return binary(new GenericDatumReader<>(schema), ofNullable(supplier));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param supplier when not null, used for record instantiation
   * @param <M> Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <M> /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<GenericRecord, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema schema,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return binary(anoaHandler, new GenericDatumReader<>(schema), ofNullable(supplier));
  }

  /**
   * @param writer Avro schema with which the record was originally serialized
   * @param reader Avro schema to use for deserialization
   * @param supplier when not null, used for record instantiation
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public /*@NonNull*/ Function<byte[], GenericRecord> binary(
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return binary(new GenericDatumReader<>(writer, reader), ofNullable(supplier));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer Avro schema with which the record was originally serialized
   * @param reader Avro schema to use for deserialization
   * @param supplier when not null, used for record instantiation
   * @param <M> Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <M> /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<GenericRecord, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return binary(anoaHandler, new GenericDatumReader<>(writer, reader), ofNullable(supplier));
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param supplier when not null, used for record instantiation
   * @param <R> Avro SpecificRecord record type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <R extends SpecificRecord> /*@NonNull*/ Function<byte[], R> binary(
      /*@NonNull*/ Class<R> recordClass,
      /*@Nullable*/ Supplier<R> supplier) {
    return binary(new SpecificDatumReader<>(recordClass), ofNullable(supplier));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param supplier when not null, used for record instantiation
   * @param <R> Avro SpecificRecord record type
   * @param <M> Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <R extends SpecificRecord, M> /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      /*@Nullable*/ Supplier<R> supplier) {
    return binary(anoaHandler, new SpecificDatumReader<>(recordClass), ofNullable(supplier));
  }

  static protected class BinaryDecoderWrapper {

    protected BinaryDecoder decoder = null;

    protected BinaryDecoder getDecoder(byte[] bytes) {
      decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
      return decoder;
    }
  }

  static <R extends IndexedRecord> /*@NonNull*/ Function<byte[], R> binary(
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ Supplier<R> supplier) {
    BinaryDecoderWrapper decoderWrapper = new BinaryDecoderWrapper();
    return (byte[] in) -> {
      try {
        return reader.read(supplier.get(), decoderWrapper.getDecoder(in));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  static <R extends IndexedRecord, M> /*@NonNull*/ Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ Supplier<R> supplier) {
    BinaryDecoderWrapper decoderWrapper = new BinaryDecoderWrapper();
    return anoaHandler.functionChecked(
        (byte[] in) -> reader.read(supplier.get(), decoderWrapper.getDecoder(in)));
  }

  /**
   * @param schema Avro record schema
   * @param supplier when not null, used for record instantiation
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public /*@NonNull*/ Function<String, GenericRecord> json(
      /*@NonNull*/ Schema schema,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return json(new GenericDatumReader<>(schema), ofNullable(supplier));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param supplier when not null, used for record instantiation
   * @param <M> Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <M> /*@NonNull*/ Function<Anoa<String, M>, Anoa<GenericRecord, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema schema,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return json(anoaHandler, new GenericDatumReader<>(schema), ofNullable(supplier));
  }

  /**
   * @param writer Avro schema with which the record was originally serialized
   * @param reader Avro schema to use for deserialization
   * @param supplier when not null, used for record instantiation
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public /*@NonNull*/ Function<String, GenericRecord> json(
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return json(new GenericDatumReader<>(writer, reader), ofNullable(supplier));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer Avro schema with which the record was originally serialized
   * @param reader Avro schema to use for deserialization
   * @param supplier when not null, used for record instantiation
   * @param <M> Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <M> /*@NonNull*/ Function<Anoa<String, M>, Anoa<GenericRecord, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema writer,
      /*@NonNull*/ Schema reader,
      /*@Nullable*/ Supplier<GenericRecord> supplier) {
    return json(anoaHandler, new GenericDatumReader<>(writer, reader), ofNullable(supplier));
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param supplier when not null, used for record instantiation
   * @param <R> Avro SpecificRecord record type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <R extends SpecificRecord> /*@NonNull*/ Function<String, R> json(
      /*@NonNull*/ Class<R> recordClass,
      /*@Nullable*/ Supplier<R> supplier) {
    return json(new SpecificDatumReader<>(recordClass), ofNullable(supplier));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param supplier when not null, used for record instantiation
   * @param <R> Avro SpecificRecord record type
   * @param <M> Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <R extends SpecificRecord, M> /*@NonNull*/ Function<Anoa<String, M>, Anoa<R, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      /*@Nullable*/ Supplier<R> supplier) {
    return json(anoaHandler, new SpecificDatumReader<>(recordClass), ofNullable(supplier));
  }

  static protected JsonDecoder createJsonDecoder(Schema schema) {
    try {
      return DecoderFactory.get().jsonDecoder(schema, "");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static <R extends IndexedRecord> /*@NonNull*/ Function<String, R> json(
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ Supplier<R> supplier) {
    final JsonDecoder jsonDecoder = createJsonDecoder(reader.getSchema());
    return (String in) -> {
      try {
        jsonDecoder.configure(in);
        return reader.read(supplier.get(), jsonDecoder);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  static <R extends IndexedRecord, M> /*@NonNull*/ Function<Anoa<String, M>, Anoa<R, M>> json(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ GenericDatumReader<R> reader,
      /*@NonNull*/ Supplier<R> supplier) {
    final JsonDecoder jsonDecoder = createJsonDecoder(reader.getSchema());
    return anoaHandler.functionChecked((String in) -> {
      jsonDecoder.configure(in);
      return reader.read(supplier.get(), jsonDecoder);
    });
  }

  /**
   * @param schema Avro record schema
   * @param strict enable strict type checking
   * @param <P> Jackson JsonParser type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding
   */
  static public <P extends JsonParser> /*@NonNull*/ Function<P, GenericRecord> jackson(
      /*@NonNull*/ Schema schema,
      boolean strict) {
    final AvroReader<GenericRecord> reader = new AvroReader.GenericReader(schema);
    return (P jp) -> reader.read(jp, strict);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record Schema
   * @param strict enable strict type checking
   * @param <P> Jackson JsonParser type
   * @param <M> Metadata type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding
   */
  static public <P extends JsonParser, M>
  /*@NonNull*/ Function<Anoa<P, M>, Anoa<GenericRecord, M>> jackson(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Schema schema,
      boolean strict) {
    final AvroReader<GenericRecord> reader = new AvroReader.GenericReader(schema);
    return anoaHandler.functionChecked((P jp) -> reader.readChecked(jp, strict));
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param strict enable strict type checking
   * @param <P> Jackson JsonParser type
   * @param <R> Avro SpecificRecord record type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding
   */
  static public <P extends JsonParser, R extends SpecificRecord> /*@NonNull*/ Function<P, R> jackson(
      /*@NonNull*/ Class<R> recordClass,
      boolean strict) {
    final AvroReader<R> reader = new AvroReader.SpecificReader<>(recordClass);
    return (P jp) -> reader.read(jp, strict);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param strict enable strict type checking
   * @param <P> Jackson JsonParser type
   * @param <R> Avro SpecificRecord record type
   * @param <M> Metadata type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding
   */
  static public <P extends JsonParser, R extends SpecificRecord, M>
  /*@NonNull*/ Function<Anoa<P, M>, Anoa<R, M>> jackson(
      /*@NonNull*/ AnoaHandler<M> anoaHandler,
      /*@NonNull*/ Class<R> recordClass,
      boolean strict) {
    final AvroReader<R> reader = new AvroReader.SpecificReader<>(recordClass);
    return anoaHandler.functionChecked((P jp) -> reader.readChecked(jp, strict));
  }
}
