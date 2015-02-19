package com.adgear.anoa.write;

import com.adgear.anoa.AnoaFunction;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class AnoaWrite {

  static public <T> AnoaFunction<T, TokenBuffer> anoaFn(Class<T> avroOrThriftClass) {
    return AnoaFunction.pokemonize(fnWrap(biCo(avroOrThriftClass)),
                                   TBase.class.isAssignableFrom(avroOrThriftClass)
                                   ? ThriftWriter.class
                                   : AvroWriter.class);
  }

  static public AnoaFunction<GenericRecord, TokenBuffer> anoaFn(Schema avroSchema, Class context) {
    return AnoaFunction.pokemonize(fnWrap(biCo(avroSchema)), AvroWriter.class);
  }

  static public <T> Function<T, TokenBuffer> fn(Class<T> avroOrThriftClass) {
    return fnWrap(biCo(avroOrThriftClass));
  }

  static public Function<GenericRecord, TokenBuffer> fn(Schema avroSchema) {
    return fnWrap(biCo(avroSchema));
  }

  @SuppressWarnings("unchecked")
  static public <T> BiConsumer<T, JsonGenerator> biCo(Class<T> avroOrThriftClass) {
    if (TBase.class.isAssignableFrom(avroOrThriftClass)) {
      return cache.computeIfAbsent(avroOrThriftClass, __ -> new ThriftWriter(avroOrThriftClass));
    } else if (SpecificRecord.class.isAssignableFrom(avroOrThriftClass)) {
      return cache.computeIfAbsent(avroOrThriftClass, __ -> new AvroWriter(avroOrThriftClass));
    } else {
      throw new IllegalArgumentException("Not Thrift or Avro record: " + avroOrThriftClass);
    }
  }

  @SuppressWarnings("unchecked")
  static public BiConsumer<GenericRecord, JsonGenerator> biCo(Schema avroSchema) {
    return cache.computeIfAbsent(avroSchema, __ -> new AvroWriter(avroSchema));
  }

  static private ConcurrentHashMap<Object, BiConsumer> cache = new ConcurrentHashMap<>();
  static private ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static private <T> Function<T, TokenBuffer> fnWrap(BiConsumer<T, JsonGenerator> biCo) {
    return ((T t) -> {
      final TokenBuffer tb = new TokenBuffer(OBJECT_MAPPER, false);
      biCo.accept(t, tb);
      try {
        tb.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return tb;
    });
  }
}
