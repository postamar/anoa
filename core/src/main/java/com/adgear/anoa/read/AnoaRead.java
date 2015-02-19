package com.adgear.anoa.read;

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

  static public <T> AnoaFunction<JsonParser,T> anoaFn(Class<T> avroOfThriftClass, boolean strict) {
    final Function<JsonParser,T> fn = fn(avroOfThriftClass, strict);
    return AnoaFunction.pokemonize(fn, TBase.class.isAssignableFrom(avroOfThriftClass)
                                       ? ThriftReader.class
                                       : AvroReader.SpecificReader.class);
  }

  static public AnoaFunction<JsonParser,GenericRecord> anoaFn(Schema avroSchema, boolean strict) {
    final Function<JsonParser,GenericRecord> fn = fn(avroSchema, strict);
    return AnoaFunction.pokemonize(fn, AvroReader.GenericReader.class);
  }

  static public <T> Function<JsonParser,T> fn(Class<T> avroOfThriftClass, boolean strict) {
    final BiFunction<JsonParser,Boolean,T> fn = biFn(avroOfThriftClass);
    return jp -> fn.apply(jp, strict);
  }

  static public Function<JsonParser,GenericRecord> fn(Schema avroSchema, boolean strict) {
    final BiFunction<JsonParser,Boolean,GenericRecord> fn = biFn(avroSchema);
    return jp -> fn.apply(jp, strict);
  }

  @SuppressWarnings("unchecked")
  static public <T> BiFunction<JsonParser,Boolean,T> biFn(Class<T> avroOrThriftClass) {
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
  static public BiFunction<JsonParser,Boolean,GenericRecord> biFn(Schema avroSchema) {
    return cache.computeIfAbsent(avroSchema, __ -> new AvroReader.GenericReader(avroSchema));
  }

  static private ConcurrentHashMap<Object,BiFunction> cache = new ConcurrentHashMap<>();
}
