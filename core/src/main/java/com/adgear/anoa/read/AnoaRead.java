package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaFunction;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

public class AnoaRead {

  static public <T> @NonNull AnoaFunction<JsonParser,T> anoaFn(@NonNull Class<T> avroOfThriftClass,
                                                               boolean strict) {
    final Function<JsonParser,T> fn = fn(avroOfThriftClass, strict);
    return AnoaFunction.pokemonize(fn, TBase.class.isAssignableFrom(avroOfThriftClass)
                                       ? ThriftReader.class
                                       : AvroReader.SpecificReader.class);
  }

  static public @NonNull AnoaFunction<JsonParser,GenericRecord> anoaFn(@NonNull Schema avroSchema,
                                                                       boolean strict) {
    final Function<JsonParser,GenericRecord> fn = fn(avroSchema, strict);
    return AnoaFunction.pokemonize(fn, AvroReader.GenericReader.class);
  }

  static public <T> @NonNull Function<JsonParser,T> fn(@NonNull Class<T> avroOfThriftClass,
                                                       boolean strict) {
    final BiFunction<JsonParser,Boolean,T> fn = biFn(avroOfThriftClass);
    return jp -> fn.apply(jp, strict);
  }

  static public @NonNull Function<JsonParser,GenericRecord> fn(@NonNull Schema avroSchema,
                                                               boolean strict) {
    final BiFunction<JsonParser,Boolean,GenericRecord> fn = biFn(avroSchema);
    return jp -> fn.apply(jp, strict);
  }

  @SuppressWarnings("unchecked")
  static public <T> @NonNull BiFunction<JsonParser,Boolean,T> biFn(
      @NonNull Class<T> avroOrThriftClass) {
    if (TBase.class.isAssignableFrom(avroOrThriftClass)) {
      return cache.computeIfAbsent(avroOrThriftClass,
                                   __ -> new ThriftReader(avroOrThriftClass));
    } else if (SpecificRecord.class.isAssignableFrom(avroOrThriftClass)) {
      return cache.computeIfAbsent(avroOrThriftClass,
                                   __ -> new AvroReader.SpecificReader(avroOrThriftClass));
    } else {
      throw new IllegalArgumentException("Not Thrift or Avro record: " + avroOrThriftClass);
    }
  }

  @SuppressWarnings("unchecked")
  static public @NonNull BiFunction<JsonParser,Boolean,GenericRecord> biFn(
      @NonNull Schema avroSchema) {
    return cache.computeIfAbsent(avroSchema, __ -> new AvroReader.GenericReader(avroSchema));
  }

  static private ConcurrentHashMap<Object,BiFunction> cache = new ConcurrentHashMap<>();
}
