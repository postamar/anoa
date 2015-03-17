package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.jooq.lambda.Unchecked;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class AvroDecoders {

  static public <R extends IndexedRecord> @NonNull Function<byte[], R> binary(
      @NonNull DatumReader<R> reader) {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    return fn(reader, bytes -> DecoderFactory.get().binaryDecoder(bytes, decoder));
  }

  static public <R extends IndexedRecord, M>
  @NonNull Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DatumReader<R> reader) {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    return fn(anoaFactory, reader, bytes -> DecoderFactory.get().binaryDecoder(bytes, decoder));
  }

  static public <R extends IndexedRecord> @NonNull Function<byte[], R> binary(
      @NonNull DatumReader<R> reader,
      @NonNull Supplier<R> supplier) {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    return fn(reader, supplier, bytes -> DecoderFactory.get().binaryDecoder(bytes, decoder));
  }

  static public <R extends IndexedRecord, M>
  @NonNull Function<Anoa<byte[], M>, Anoa<R, M>> binary(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DatumReader<R> reader,
      @NonNull Supplier<R> supplier) {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    return fn(anoaFactory,
              reader,
              supplier,
              bytes -> DecoderFactory.get().binaryDecoder(bytes, decoder));
  }

  static public <R extends IndexedRecord> @NonNull Function<String, R> json(
      @NonNull GenericDatumReader<R> reader) {
    Schema schema = reader.getSchema();
    return fn(reader, Unchecked.function(s -> DecoderFactory.get().jsonDecoder(schema, s)));
  }

  static public <R extends IndexedRecord, M>
  @NonNull Function<Anoa<String, M>, Anoa<R, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull GenericDatumReader<R> reader) {
    Schema schema = reader.getSchema();
    return fn(anoaFactory,
              reader,
              Unchecked.function(s -> DecoderFactory.get().jsonDecoder(schema, s)));
  }

  static public <R extends IndexedRecord> @NonNull Function<String, R> json(
      @NonNull DatumReader<R> reader,
      @NonNull Supplier<R> supplier) {
    Schema schema = supplier.get().getSchema();
    return fn(reader,
              supplier,
              Unchecked.function(s -> DecoderFactory.get().jsonDecoder(schema, s)));
  }

  static public <R extends IndexedRecord, M>
  @NonNull Function<Anoa<String, M>, Anoa<R, M>> json(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DatumReader<R> reader,
      @NonNull Supplier<R> supplier) {
    Schema schema = supplier.get().getSchema();
    return fn(anoaFactory,
              reader,
              supplier,
              Unchecked.function(s -> DecoderFactory.get().jsonDecoder(schema, s)));
  }

  static public <T, R extends IndexedRecord, D extends Decoder> @NonNull Function<T, R> fn(
      @NonNull DatumReader<R> reader,
      @NonNull Function<T, D> decoderFactory) {
    return (T in) -> {
      try {
        return reader.read(null, decoderFactory.apply(in));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  static public <T, R extends IndexedRecord, D extends Decoder, M>
  @NonNull Function<Anoa<T, M>, Anoa<R, M>> fn(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DatumReader<R> reader,
      @NonNull Function<T, D> decoderFactory) {
    return anoaFactory.functionChecked((T in) -> reader.read(null, decoderFactory.apply(in)));
  }

  static public <T, R extends IndexedRecord, D extends Decoder> @NonNull Function<T, R> fn(
      @NonNull DatumReader<R> reader,
      @NonNull Supplier<R> supplier,
      @NonNull Function<T, D> decoderFactory) {
    return (T in) -> {
      try {
        return reader.read(supplier.get(), decoderFactory.apply(in));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  static public <T, R extends IndexedRecord, D extends Decoder, M>
  @NonNull Function<Anoa<T, M>, Anoa<R, M>> fn(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull DatumReader<R> reader,
      @NonNull Supplier<R> supplier,
      @NonNull Function<T, D> decoderFactory) {
    return anoaFactory.functionChecked(
        (T in) -> reader.read(supplier.get(), decoderFactory.apply(in)));
  }

  static public <P extends JsonParser> @NonNull Function<P, GenericRecord> jackson(
      @NonNull Schema schema,
      boolean strict) {
    final AvroReader<GenericRecord> reader = new AvroReader.GenericReader(schema);
    return (P jp) -> reader.read(jp, strict);
  }

  static public <P extends JsonParser, M>
  @NonNull Function<Anoa<P, M>, Anoa<GenericRecord, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Schema schema,
      boolean strict) {
    final AvroReader<GenericRecord> reader = new AvroReader.GenericReader(schema);
    return anoaFactory.functionChecked((P jp) -> reader.readChecked(jp, strict));
  }

  static public <P extends JsonParser, R extends SpecificRecord> @NonNull Function<P, R> jackson(
      @NonNull Class<R> recordClass,
      boolean strict) {
    final AvroReader<R> reader = new AvroReader.SpecificReader<>(recordClass);
    return (P jp) -> reader.read(jp, strict);
  }

  static public <P extends JsonParser, R extends SpecificRecord, M>
  @NonNull Function<Anoa<P, M>, Anoa<R, M>> jackson(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull Class<R> recordClass,
      boolean strict) {
    final AvroReader<R> reader = new AvroReader.SpecificReader<>(recordClass);
    return anoaFactory.functionChecked((P jp) -> reader.readChecked(jp, strict));
  }
}
