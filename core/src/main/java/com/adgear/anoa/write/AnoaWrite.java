package com.adgear.anoa.write;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaFunction;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class AnoaWrite {

  static public <T, JG extends JsonGenerator> @NonNull AnoaFunction<T, JG> anoaFn(
      @NonNull Class<T> avroOrThriftClass,
      @NonNull Supplier<@NonNull JG> jsonGeneratorSupplier) {
    return AnoaFunction.pokemonize(fnWrap(biCo(avroOrThriftClass), jsonGeneratorSupplier),
                                   TBase.class.isAssignableFrom(avroOrThriftClass)
                                   ? ThriftWriter.class
                                   : AvroWriter.class);
  }

  static public <JG extends JsonGenerator> @NonNull AnoaFunction<GenericRecord, JG> anoaFn(
      @NonNull Schema avroSchema,
      @NonNull Supplier<@NonNull JG> jsonGeneratorSupplier,
      Class context) {
    return AnoaFunction.pokemonize(fnWrap(biCo(avroSchema), jsonGeneratorSupplier),
                                   AvroWriter.class);
  }

  static public <T, JG extends JsonGenerator> @NonNull Function<T, JG> fn(
      @NonNull Class<T> avroOrThriftClass,
      @NonNull Supplier<@NonNull JG> jsonGeneratorSupplier) {
    return fnWrap(biCo(avroOrThriftClass), jsonGeneratorSupplier);
  }

  static public <JG extends JsonGenerator> @NonNull Function<GenericRecord, JG> fn(
      @NonNull Schema avroSchema,
      @NonNull Supplier<@NonNull JG> jsonGeneratorSupplier) {
    return fnWrap(biCo(avroSchema), jsonGeneratorSupplier);
  }

  @SuppressWarnings("unchecked")
  static public <T> @NonNull BiConsumer<T, JsonGenerator> biCo(
      @NonNull Class<T> avroOrThriftClass) {
    if (TBase.class.isAssignableFrom(avroOrThriftClass)) {
      return cache.computeIfAbsent(avroOrThriftClass, __ -> new ThriftWriter(avroOrThriftClass));
    } else if (SpecificRecord.class.isAssignableFrom(avroOrThriftClass)) {
      return cache.computeIfAbsent(avroOrThriftClass, __ -> new AvroWriter(avroOrThriftClass));
    } else {
      throw new IllegalArgumentException("Not Thrift or Avro record: " + avroOrThriftClass);
    }
  }

  @SuppressWarnings("unchecked")
  static public @NonNull BiConsumer<GenericRecord, JsonGenerator> biCo(@NonNull Schema avroSchema) {
    return cache.computeIfAbsent(avroSchema, __ -> new AvroWriter(avroSchema));
  }

  static private ConcurrentHashMap<Object, BiConsumer> cache = new ConcurrentHashMap<>();

  static private <T, JG extends JsonGenerator> Function<T, JG> fnWrap(
      BiConsumer<T, JsonGenerator> biCo,
      Supplier<JG> jgSupplier) {
    return ((T t) -> {
      final JG jg = jgSupplier.get();
      biCo.accept(t, jg);
      try {
        jg.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return jg;
    });
  }
}
