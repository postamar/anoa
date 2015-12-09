package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility class for generating functions for serializing Avro records. Unless specified otherwise,
 * the functions should not be deemed thread-safe.
 */
public class AvroEncoders {

  protected AvroEncoders() {
  }

  /**
   * @param schema Avro schema of records to serialize
   */
  static public Function<GenericRecord, byte[]> binary(
      Schema schema) {
    return binary(new GenericDatumWriter<>(schema));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro schema of records to serialize
   * @param <M> Metadata type
   */
  static public <M> Function<Anoa<GenericRecord, M>, Anoa<byte[], M>> binary(
      AnoaHandler<M> anoaHandler,
      Schema schema) {
    return binary(anoaHandler, new GenericDatumWriter<>(schema));
  }

  /**
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> Function<R, byte[]> binary(
      Class<R> recordClass) {
    return binary(new SpecificDatumWriter<>(recordClass));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> Function<Anoa<R, M>, Anoa<byte[], M>> binary(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    return binary(anoaHandler, new SpecificDatumWriter<>(recordClass));
  }

  static <R extends IndexedRecord> Function<R, byte[]> binary(
      DatumWriter<R> writer) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    return (R record) -> {
      baos.reset();
      try {
        writer.write(record, encoder);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return baos.toByteArray();
    };
  }

  static <R extends IndexedRecord, M> Function<Anoa<R, M>, Anoa<byte[], M>> binary(
      AnoaHandler<M> anoaHandler,
      DatumWriter<R> writer) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    return anoaHandler.functionChecked((R record) -> {
      baos.reset();
      writer.write(record, encoder);
      return baos.toByteArray();
    });
  }

  /**
   * @param schema Avro schema of records to serialize
   */
  static public Function<GenericRecord, String> json(
      Schema schema) {
    return json(new GenericDatumWriter<GenericRecord>(schema), schema);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro schema of records to serialize
   * @param <M> Metadata type
   */
  static public <M> Function<Anoa<GenericRecord, M>, Anoa<String, M>> json(
      AnoaHandler<M> anoaHandler,
      Schema schema) {
    return json(anoaHandler, new GenericDatumWriter<GenericRecord>(schema), schema);
  }

  /**
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> Function<R, String> json(
      Class<R> recordClass) {
    final SpecificDatumWriter<R> writer = new SpecificDatumWriter<>(recordClass);
    return json(writer, writer.getSpecificData().getSchema(recordClass));
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> Function<Anoa<R, M>, Anoa<String, M>> json(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass) {
    final SpecificDatumWriter<R> writer = new SpecificDatumWriter<>(recordClass);
    return json(anoaHandler, writer, writer.getSpecificData().getSchema(recordClass));
  }

  static <R extends IndexedRecord> Function<R, String> json(
      DatumWriter<R> writer,
      Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Encoder encoder;
    try {
      encoder = EncoderFactory.get().jsonEncoder(schema, baos);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return (R record) -> {
      baos.reset();
      try {
        writer.write(record, encoder);
        encoder.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return baos.toString();
    };
  }

  static <R extends IndexedRecord, M> Function<Anoa<R, M>, Anoa<String, M>> json(
      AnoaHandler<M> anoaHandler,
      DatumWriter<R> writer,
      Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Encoder encoder;
    try {
      encoder = EncoderFactory.get().jsonEncoder(schema, baos);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return anoaHandler.functionChecked((R record) -> {
      baos.reset();
      writer.write(record, encoder);
      encoder.flush();
      return baos.toString();
    });
  }

  /**
   * @param recordClass Class object of Avro records to be serialized
   * @param supplier called for each new record serialization
   * @param <R> Avro record type
   * @param <G> JsonGenerator type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <R extends SpecificRecord, G extends JsonGenerator> Function<R, G> jackson(
      Class<R> recordClass,
      Supplier<G> supplier) {
    return jackson(new AvroWriter<>(recordClass), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param recordClass Class object of Avro records to be serialized
   * @param supplier called for each new record serialization
   * @param <R> Avro record Type
   * @param <G> JsonGenerator type
   * @param <M> Metadata type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <R extends SpecificRecord, G extends JsonGenerator, M>
  Function<Anoa<R, M>, Anoa<G, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Class<R> recordClass,
      Supplier<G> supplier) {
    return jackson(anoaHandler, new AvroWriter<>(recordClass), supplier);
  }

  /**
   * @param schema Avro record schema
   * @param supplier called for each new record serialization
   * @param <G> JsonGenerator type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <G extends JsonGenerator> Function<GenericRecord, G> jackson(
      Schema schema,
      Supplier<G> supplier) {
    return jackson(new AvroWriter<>(schema), supplier);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param schema Avro record schema
   * @param supplier called for each new record serialization
   * @param <G> JsonGenerator type
   * @param <M> Metadata type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <G extends JsonGenerator, M>
  Function<Anoa<GenericRecord, M>, Anoa<G, M>> jackson(
      AnoaHandler<M> anoaHandler,
      Schema schema,
      Supplier<G> supplier) {
    return jackson(anoaHandler, new AvroWriter<>(schema), supplier);
  }

  static <G extends JsonGenerator, R extends IndexedRecord> Function<R, G> jackson(
      AvroWriter<R> writer,
      Supplier<G> supplier) {
    return (R record) -> {
      G jg = supplier.get();
      writer.write(record, jg);
      return jg;
    };
  }

  static <G extends JsonGenerator, R extends IndexedRecord, M>
  Function<Anoa<R, M>, Anoa<G, M>> jackson(
      AnoaHandler<M> anoaHandler,
      AvroWriter<R> writer,
      Supplier<G> supplier) {
    return anoaHandler.functionChecked((R record) -> {
      G jg = supplier.get();
      writer.writeChecked(record, jg);
      return jg;
    });
  }
}
