package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaFunction;
import com.adgear.anoa.factory.util.CachedFactory;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;

import java.util.function.BiFunction;
import java.util.function.Function;

public class AnoaRead {

  static public <T> @NonNull AnoaFunction<JsonParser, T> anoaFn(@NonNull Class<T> avroOfThriftClass,
                                                                boolean strict) {
    final Function<JsonParser, T> fn = fn(avroOfThriftClass, strict);
    return AnoaFunction.pokemon(fn, TBase.class.isAssignableFrom(avroOfThriftClass)
                                    ? ThriftReader.class
                                    : AvroReader.SpecificReader.class);
  }

  static public @NonNull AnoaFunction<JsonParser, GenericRecord> anoaFn(@NonNull Schema avroSchema,
                                                                        boolean strict) {
    final Function<JsonParser, GenericRecord> fn = fn(avroSchema, strict);
    return AnoaFunction.pokemon(fn, AvroReader.GenericReader.class);
  }

  static public <T> @NonNull Function<JsonParser, T> fn(@NonNull Class<T> avroOfThriftClass,
                                                        boolean strict) {
    final BiFunction<JsonParser, Boolean, T> fn = biFn(avroOfThriftClass);
    return jp -> fn.apply(jp, strict);
  }

  static public @NonNull Function<JsonParser, GenericRecord> fn(@NonNull Schema avroSchema,
                                                                boolean strict) {
    final BiFunction<JsonParser, Boolean, GenericRecord> fn = biFn(avroSchema);
    return jp -> fn.apply(jp, strict);
  }

  @SuppressWarnings("unchecked")
  static public <T> @NonNull BiFunction<JsonParser, Boolean, T> biFn(
      @NonNull Class<T> avroOrThriftClass) {
    return (BiFunction<JsonParser, Boolean, T>) objectFactory.get(avroOrThriftClass);
  }

  static public @NonNull BiFunction<JsonParser, Boolean, GenericRecord> biFn(
      @NonNull Schema avroSchema) {
    return genericFactory.get(avroSchema);
  }

  static final CachedFactory<Schema, BiFunction<JsonParser, Boolean, GenericRecord>> genericFactory
      = new CachedFactory<>(AvroReader.GenericReader::new);
  static final CachedFactory<Class, BiFunction<JsonParser, Boolean, ?>> objectFactory
      = new CachedFactory<>(AnoaRead::createReader);

  @SuppressWarnings("unchecked")
  static private BiFunction<JsonParser, Boolean, ?> createReader(Class avroOrThriftClass) {
    if (TBase.class.isAssignableFrom(avroOrThriftClass)) {
      return new ThriftReader(avroOrThriftClass);
    } else if (SpecificRecord.class.isAssignableFrom(avroOrThriftClass)) {
      return new AvroReader.SpecificReader(avroOrThriftClass);
    }
    throw new IllegalArgumentException("Not Thrift or Avro record / schema: " + avroOrThriftClass);
  }
}
