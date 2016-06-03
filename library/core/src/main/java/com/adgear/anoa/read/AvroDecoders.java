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
final public class AvroDecoders {

  private AvroDecoders() {
  }

  /**
   * @param schema   Avro record schema
   * @param supplier used for record instantiation
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public Function<byte[], GenericRecord> binary(
      Schema schema,
      Supplier<GenericRecord> supplier) {
    return binary(new GenericDatumReader<>(schema), supplier);
  }

  /**
   * @param schema Avro record schema
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public Function<byte[], GenericRecord> binary(
      Schema schema) {
    return binary(new GenericDatumReader<>(schema));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param supplier    used for record instantiation
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <M> Function<Anoa<byte[], M>, Anoa<GenericRecord, M>> binary(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      Supplier<GenericRecord> supplier) {
    return binary(anoaHandler, new GenericDatumReader<>(schema), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <M> Function<Anoa<byte[], M>, Anoa<GenericRecord, M>> binary(
      AnoaHandler<M> anoaHandler,
      Schema schema) {
    return binary(anoaHandler, new GenericDatumReader<>(schema));
  }

  /**
   * @param writer   Avro schema with which the record was originally serialized
   * @param reader   Avro schema to use for deserialization
   * @param supplier used for record instantiation
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public Function<byte[], GenericRecord> binary(
      Schema writer,
      Schema reader,
      Supplier<GenericRecord> supplier) {
    return binary(new GenericDatumReader<>(writer, reader), supplier);
  }

  /**
   * @param writer Avro schema with which the record was originally serialized
   * @param reader Avro schema to use for deserialization
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public Function<byte[], GenericRecord> binary(
      Schema writer,
      Schema reader) {
    return binary(new GenericDatumReader<>(writer, reader));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param supplier    used for record instantiation
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <M> Function<Anoa<byte[], M>, Anoa<GenericRecord, M>> binary(
      AnoaHandler<M> anoaHandler,
      Schema writer,
      Schema reader,
      Supplier<GenericRecord> supplier) {
    return binary(anoaHandler, new GenericDatumReader<>(writer, reader), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <M> Function<Anoa<byte[], M>, Anoa<GenericRecord, M>> binary(
      AnoaHandler<M> anoaHandler,
      Schema writer,
      Schema reader) {
    return binary(anoaHandler, new GenericDatumReader<>(writer, reader));
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param supplier    used for record instantiation
   * @param <R>         Avro SpecificRecord record type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <R extends SpecificRecord> Function<byte[], R> binary(
      Class<R> recordClass,
      Supplier<R> supplier) {
    return binary(new SpecificDatumReader<>(recordClass), supplier);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param <R>         Avro SpecificRecord record type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <R extends SpecificRecord> Function<byte[], R> binary(
      Class<R> recordClass) {
    return binary(new SpecificDatumReader<>(recordClass));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param supplier    used for record instantiation
   * @param <R>         Avro SpecificRecord record type
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <R extends SpecificRecord, M> Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      Supplier<R> supplier) {
    return binary(anoaHandler, new SpecificDatumReader<>(recordClass), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param <R>         Avro SpecificRecord record type
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its binary encoding
   */
  static public <R extends SpecificRecord, M> Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    return binary(anoaHandler, new SpecificDatumReader<>(recordClass));
  }

  static <R extends IndexedRecord> Function<byte[], R> binary(
      GenericDatumReader<R> reader,
      Supplier<R> supplier) {
    BinaryDecoderWrapper decoderWrapper = new BinaryDecoderWrapper();
    return (byte[] in) -> {
      try {
        return reader.read(supplier.get(), decoderWrapper.getDecoder(in));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  static <R extends IndexedRecord> Function<byte[], R> binary(
      GenericDatumReader<R> reader) {
    return binary(reader, () -> ((R) null));
  }

  static <R extends IndexedRecord, M> Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      GenericDatumReader<R> reader,
      Supplier<R> supplier) {
    BinaryDecoderWrapper decoderWrapper = new BinaryDecoderWrapper();
    return anoaHandler.functionChecked(
        (byte[] in) -> reader.read(supplier.get(), decoderWrapper.getDecoder(in)));
  }

  static <R extends IndexedRecord, M> Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      AnoaHandler<M> anoaHandler,
      GenericDatumReader<R> reader) {
    return binary(anoaHandler, reader, () -> ((R) null));
  }

  /**
   * @param schema   Avro record schema
   * @param supplier used for record instantiation
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public Function<String, GenericRecord> json(
      Schema schema,
      Supplier<GenericRecord> supplier) {
    return json(new GenericDatumReader<>(schema), supplier);
  }

  /**
   * @param schema Avro record schema
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public Function<String, GenericRecord> json(
      Schema schema) {
    return json(new GenericDatumReader<>(schema));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param supplier    used for record instantiation
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <M> Function<Anoa<String, M>, Anoa<GenericRecord, M>> json(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      Supplier<GenericRecord> supplier) {
    return json(anoaHandler, new GenericDatumReader<>(schema), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record schema
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <M> Function<Anoa<String, M>, Anoa<GenericRecord, M>> json(
      AnoaHandler<M> anoaHandler,
      Schema schema) {
    return json(anoaHandler, new GenericDatumReader<>(schema));
  }

  /**
   * @param writer   Avro schema with which the record was originally serialized
   * @param reader   Avro schema to use for deserialization
   * @param supplier used for record instantiation
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public Function<String, GenericRecord> json(
      Schema writer,
      Schema reader,
      Supplier<GenericRecord> supplier) {
    return json(new GenericDatumReader<>(writer, reader), supplier);
  }

  /**
   * @param writer Avro schema with which the record was originally serialized
   * @param reader Avro schema to use for deserialization
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public Function<String, GenericRecord> json(
      Schema writer,
      Schema reader) {
    return json(new GenericDatumReader<>(writer, reader));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param supplier    used for record instantiation
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <M> Function<Anoa<String, M>, Anoa<GenericRecord, M>> json(
      AnoaHandler<M> anoaHandler,
      Schema writer,
      Schema reader,
      Supplier<GenericRecord> supplier) {
    return json(anoaHandler, new GenericDatumReader<>(writer, reader), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param writer      Avro schema with which the record was originally serialized
   * @param reader      Avro schema to use for deserialization
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <M> Function<Anoa<String, M>, Anoa<GenericRecord, M>> json(
      AnoaHandler<M> anoaHandler,
      Schema writer,
      Schema reader) {
    return json(anoaHandler, new GenericDatumReader<>(writer, reader));
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param supplier    used for record instantiation
   * @param <R>         Avro SpecificRecord record type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <R extends SpecificRecord> Function<String, R> json(
      Class<R> recordClass,
      Supplier<R> supplier) {
    return json(new SpecificDatumReader<>(recordClass), supplier);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param <R>         Avro SpecificRecord record type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <R extends SpecificRecord> Function<String, R> json(
      Class<R> recordClass) {
    return json(new SpecificDatumReader<>(recordClass));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param supplier    used for record instantiation
   * @param <R>         Avro SpecificRecord record type
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <R extends SpecificRecord, M> Function<Anoa<String, M>, Anoa<R, M>> json(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      Supplier<R> supplier) {
    return json(anoaHandler, new SpecificDatumReader<>(recordClass), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param <R>         Avro SpecificRecord record type
   * @param <M>         Metadata type
   * @return A function which deserializes an Avro record from its JSON encoding
   */
  static public <R extends SpecificRecord, M> Function<Anoa<String, M>, Anoa<R, M>> json(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    return json(anoaHandler, new SpecificDatumReader<>(recordClass));
  }

  static protected JsonDecoder createJsonDecoder(Schema schema) {
    try {
      return DecoderFactory.get().jsonDecoder(schema, "");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static <R extends IndexedRecord> Function<String, R> json(
      GenericDatumReader<R> reader,
      Supplier<R> supplier) {
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

  static <R extends IndexedRecord> Function<String, R> json(
      GenericDatumReader<R> reader) {
    return json(reader, () -> ((R) null));
  }

  static <R extends IndexedRecord, M> Function<Anoa<String, M>, Anoa<R, M>> json(
      AnoaHandler<M> anoaHandler,
      GenericDatumReader<R> reader,
      Supplier<R> supplier) {
    final JsonDecoder jsonDecoder = createJsonDecoder(reader.getSchema());
    return anoaHandler.functionChecked((String in) -> {
      jsonDecoder.configure(in);
      return reader.read(supplier.get(), jsonDecoder);
    });
  }

  static <R extends IndexedRecord, M> Function<Anoa<String, M>, Anoa<R, M>> json(
      AnoaHandler<M> anoaHandler,
      GenericDatumReader<R> reader) {
    return json(anoaHandler, reader, () -> ((R) null));
  }

  /**
   * @param schema Avro record schema
   * @param <P>    Jackson JsonParser type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser> Function<P, GenericRecord> jackson(
      Schema schema) {
    return new AvroReader.GenericReader(schema).decoder();
  }

  /**
   * @param schema Avro record schema
   * @param <P>    Jackson JsonParser type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding,
   * with strictest possible type checking.
   */
  static public <P extends JsonParser> Function<P, GenericRecord> jacksonStrict(
      Schema schema) {
    return new AvroReader.GenericReader(schema).decoderStrict();
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record Schema
   * @param <P>         Jackson JsonParser type
   * @param <M>         Metadata type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding.
   */
  static public <P extends JsonParser, M>
  Function<Anoa<P, M>, Anoa<GenericRecord, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Schema schema) {
    return new AvroReader.GenericReader(schema).decoder(anoaHandler);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema      Avro record Schema
   * @param <P>         Jackson JsonParser type
   * @param <M>         Metadata type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding,
   * with strictest possible type checking.
   */
  static public <P extends JsonParser, M>
  Function<Anoa<P, M>, Anoa<GenericRecord, M>> jacksonStrict(
      AnoaHandler<M> anoaHandler,
      Schema schema) {
    return new AvroReader.GenericReader(schema).decoderStrict(anoaHandler);
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Avro SpecificRecord record type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding
   */
  static public <P extends JsonParser, R extends SpecificRecord> Function<P, R> jackson(
      Class<R> recordClass) {
    return new AvroReader.SpecificReader<>(recordClass).decoder();
  }

  /**
   * @param recordClass Avro SpecificRecord class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Avro SpecificRecord record type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding,
   * with strictest possible type checking.
   */
  static public <P extends JsonParser, R extends SpecificRecord> Function<P, R> jacksonStrict(
      Class<R> recordClass) {
    return new AvroReader.SpecificReader<>(recordClass).decoderStrict();
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Avro SpecificRecord record type
   * @param <M>         Metadata type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding
   */
  static public <P extends JsonParser, R extends SpecificRecord, M>
  Function<Anoa<P, M>, Anoa<R, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    return new AvroReader.SpecificReader<>(recordClass).decoder(anoaHandler);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Avro SpecificRecord class object
   * @param <P>         Jackson JsonParser type
   * @param <R>         Avro SpecificRecord record type
   * @param <M>         Metadata type
   * @return A function which reads an Avro record from a JsonParser, in its 'natural' encoding,
   * with strictest possible type checking.
   */
  static public <P extends JsonParser, R extends SpecificRecord, M>
  Function<Anoa<P, M>, Anoa<R, M>> jacksonStrict(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    return new AvroReader.SpecificReader<>(recordClass).decoderStrict(anoaHandler);
  }

  static protected class BinaryDecoderWrapper {

    protected BinaryDecoder decoder = null;

    protected BinaryDecoder getDecoder(byte[] bytes) {
      decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
      return decoder;
    }
  }
}
