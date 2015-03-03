package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
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

  static public <R extends IndexedRecord> @NonNull Function<byte[], R> binary(
      @NonNull DatumReader<R> reader,
      @Nullable Supplier<R> supplier) {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    return fn(reader, supplier, bytes -> DecoderFactory.get().binaryDecoder(bytes, decoder));
  }

  static public <R extends IndexedRecord> @NonNull Function<String, R> json(
      @NonNull GenericDatumReader<R> reader) {
    Schema schema = reader.getSchema();
    return fn(reader, Unchecked.function(s -> DecoderFactory.get().jsonDecoder(schema, s)));
  }

  static public <R extends IndexedRecord> @NonNull Function<String, R> json(
      @NonNull DatumReader<R> reader,
      @NonNull Supplier<R> supplier) {
    Schema schema = supplier.get().getSchema();
    return fn(reader,
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
}
