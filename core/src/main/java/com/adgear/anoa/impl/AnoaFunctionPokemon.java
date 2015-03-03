package com.adgear.anoa.impl;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaCounted;
import com.adgear.anoa.AnoaRecord;

import java.util.function.Function;
import java.util.stream.Stream;

public class AnoaFunctionPokemon<T, R> extends AnoaFunctionBase<T, R> {

  final @NonNull Function<T, R> wrappedFunction;
  final Class functionContext;

  public AnoaFunctionPokemon(@NonNull Function<T, R> wrappedFunction,
                      Class functionContext) {
    this.wrappedFunction = wrappedFunction;
    this.functionContext = functionContext;
  }

  protected String toCountedLabel(@NonNull Exception e) {
    return "["
           + ((functionContext == null) ? "" : (functionContext.getCanonicalName() + ": "))
           + e.getClass().getName() + "]"
           + ((e.getMessage() == null) ? "" : (" " + e.getMessage().replace("\n","\\n ")));
  }

  @Override
  final protected AnoaRecord<R> applyPresent(@NonNull AnoaRecord<@NonNull T> record) {
    final R result;
    try {
      result = wrappedFunction.apply(record.asOptional().get());
    } catch (RuntimeException e) {
      final AnoaCounted counted = AnoaCounted.get(toCountedLabel(e));
      return AnoaRecordImpl.createEmpty(record, Stream.of(counted));
    }
    return AnoaRecordImpl.create(result, record.asCountedStream());
  }
}
