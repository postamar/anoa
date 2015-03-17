package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class AvroEncoders {

  static public <R extends IndexedRecord> @NonNull Function<R, byte[]> binary(
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

  static public <R extends IndexedRecord, M> @NonNull Function<Anoa<R, M>, Anoa<byte[], M>> binary(
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

  static public <R extends IndexedRecord> @NonNull Function<R, String> json(
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

  static public <R extends IndexedRecord, M> @NonNull Function<Anoa<R, M>, Anoa<String, M>> json(
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

  static public <R extends SpecificRecord, G extends JsonGenerator> @NonNull Function<R, G> jackson(
      @NonNull Supplier<G> supplier,
      @NonNull Class<R> recordClass) {
    return jackson(supplier, new AvroWriter<>(recordClass));
  }

  static public <R extends SpecificRecord, G extends JsonGenerator, M>
  @NonNull Function<Anoa<R, M>, Anoa<G, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<G> supplier,
      @NonNull Class<R> recordClass) {
    return jackson(anoaFactory, supplier, new AvroWriter<>(recordClass));
  }

  static public <G extends JsonGenerator> @NonNull Function<GenericRecord, G> jackson(
      @NonNull Supplier<G> supplier,
      @NonNull Schema schema) {
    return jackson(supplier, new AvroWriter<>(schema));
  }

  static public <G extends JsonGenerator, M>
  @NonNull Function<Anoa<GenericRecord, M>, Anoa<G, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<G> supplier,
      @NonNull Schema schema) {
    return jackson(anoaFactory, supplier, new AvroWriter<>(schema));
  }

  static <G extends JsonGenerator, R extends IndexedRecord> @NonNull Function<R, G> jackson(
      @NonNull Supplier<G> supplier,
      @NonNull AvroWriter<R> writer) {
    return (R record) -> {
      G jg = supplier.get();
      writer.write(record, jg);
      return jg;
    };
  }

  static <G extends JsonGenerator, R extends IndexedRecord, M>
  @NonNull Function<Anoa<R, M>, Anoa<G, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Supplier<G> supplier,
      @NonNull AvroWriter<R> writer) {
    return anoaFactory.functionChecked((R record) -> {
      G jg = supplier.get();
      writer.writeChecked(record, jg);
      return jg;
    });
  }
}
