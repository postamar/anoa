package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
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
 * Utility class for generating serializing functions for Avro records. Unless specified otherwise,
 * the functions should not be deemed thread-safe.
 */
public class AvroEncoders {

  /**
   * @param schema Avro schema of records to serialize
   */
  static public @NonNull Function<GenericRecord, byte[]> binary(
      @NonNull Schema schema) {
    return binary(new GenericDatumWriter<>(schema));
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro schema of records to serialize
   * @param <M> Metadata type
   */
  static public <M> @NonNull Function<Anoa<GenericRecord, M>, Anoa<byte[], M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema) {
    return binary(anoaFactory, new GenericDatumWriter<>(schema));
  }

  /**
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> @NonNull Function<R, byte[]> binary(
      @NonNull Class<R> recordClass) {
    return binary(new SpecificDatumWriter<>(recordClass));
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> @NonNull Function<Anoa<R, M>, Anoa<byte[], M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass) {
    return binary(anoaFactory, new SpecificDatumWriter<>(recordClass));
  }

  static <R extends IndexedRecord> @NonNull Function<R, byte[]> binary(
      @NonNull DatumWriter<R> writer) {
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

  static <R extends IndexedRecord, M> @NonNull Function<Anoa<R, M>, Anoa<byte[], M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DatumWriter<R> writer) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
    return anoaFactory.functionChecked((R record) -> {
      baos.reset();
      writer.write(record, encoder);
      return baos.toByteArray();
    });
  }

  /**
   * @param schema Avro schema of records to serialize
   */
  static public @NonNull Function<GenericRecord, String> json(
      @NonNull Schema schema) {
    return json(new GenericDatumWriter<GenericRecord>(schema), schema);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro schema of records to serialize
   * @param <M> Metadata type
   */
  static public <M> @NonNull Function<Anoa<GenericRecord, M>, Anoa<String, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema) {
    return json(anoaFactory, new GenericDatumWriter<GenericRecord>(schema), schema);
  }

  /**
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   */
  static public <R extends SpecificRecord> @NonNull Function<R, String> json(
      @NonNull Class<R> recordClass) {
    final SpecificDatumWriter<R> writer = new SpecificDatumWriter<>(recordClass);
    return json(writer, writer.getSpecificData().getSchema(recordClass));
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Class object of Avro records to be serialized
   * @param <R> Avro record type
   * @param <M> Metadata type
   */
  static public <R extends SpecificRecord, M> @NonNull Function<Anoa<R, M>, Anoa<String, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass) {
    final SpecificDatumWriter<R> writer = new SpecificDatumWriter<>(recordClass);
    return json(anoaFactory, writer, writer.getSpecificData().getSchema(recordClass));
  }

  static <R extends IndexedRecord> @NonNull Function<R, String> json(
      @NonNull DatumWriter<R> writer,
      @NonNull Schema schema) {
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

  static <R extends IndexedRecord, M> @NonNull Function<Anoa<R, M>, Anoa<String, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DatumWriter<R> writer,
      @NonNull Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Encoder encoder;
    try {
      encoder = EncoderFactory.get().jsonEncoder(schema, baos);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return anoaFactory.functionChecked((R record) -> {
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
  static public <R extends SpecificRecord, G extends JsonGenerator> @NonNull Function<R, G> jackson(
      @NonNull Class<R> recordClass,
      @NonNull Supplier<G> supplier) {
    return jackson(new AvroWriter<>(recordClass), supplier);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param recordClass Class object of Avro records to be serialized
   * @param supplier called for each new record serialization
   * @param <R> Avro record Type
   * @param <G> JsonGenerator type
   * @param <M> Metadata type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <R extends SpecificRecord, G extends JsonGenerator, M>
  @NonNull Function<Anoa<R, M>, Anoa<G, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass,
      @NonNull Supplier<G> supplier) {
    return jackson(anoaFactory, new AvroWriter<>(recordClass), supplier);
  }

  /**
   * @param schema Avro record schema
   * @param supplier called for each new record serialization
   * @param <G> JsonGenerator type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <G extends JsonGenerator> @NonNull Function<GenericRecord, G> jackson(
      @NonNull Schema schema,
      @NonNull Supplier<G> supplier) {
    return jackson(new AvroWriter<>(schema), supplier);
  }

  /**
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param schema Avro record schema
   * @param supplier called for each new record serialization
   * @param <G> JsonGenerator type
   * @param <M> Metadata type
   * @return A function which calls the supplier for a JsonGenerator object and writes the record
   * into it.
   */
  static public <G extends JsonGenerator, M>
  @NonNull Function<Anoa<GenericRecord, M>, Anoa<G, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      @NonNull Supplier<G> supplier) {
    return jackson(anoaFactory, new AvroWriter<>(schema), supplier);
  }

  static <G extends JsonGenerator, R extends IndexedRecord> @NonNull Function<R, G> jackson(
      @NonNull AvroWriter<R> writer,
      @NonNull Supplier<G> supplier) {
    return (R record) -> {
      G jg = supplier.get();
      writer.write(record, jg);
      return jg;
    };
  }

  static <G extends JsonGenerator, R extends IndexedRecord, M>
  @NonNull Function<Anoa<R, M>, Anoa<G, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull AvroWriter<R> writer,
      @NonNull Supplier<G> supplier) {
    return anoaFactory.functionChecked((R record) -> {
      G jg = supplier.get();
      writer.writeChecked(record, jg);
      return jg;
    });
  }
}
