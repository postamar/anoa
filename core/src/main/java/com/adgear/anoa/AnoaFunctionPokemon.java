package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.function.Function;
import java.util.stream.Stream;

class AnoaFunctionPokemon<T, R> extends AnoaFunctionBase<T, R> {

  final @NonNull Function<T, R> wrappedFunction;
  final Class functionContext;

  AnoaFunctionPokemon(@NonNull Function<T, R> wrappedFunction,
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
  final protected AnoaRecord<R> applyNonNull(@NonNull AnoaRecord<@NonNull T> record) {
    final R result;
    try {
      result = wrappedFunction.apply(record.get());
    } catch (RuntimeException e) {
      final AnoaCounted counted = AnoaCounted.get(toCountedLabel(e));
      return AnoaRecordImpl.create(null, Stream.concat(Stream.of(counted), record.asCountedStream()));
    }
    return AnoaRecordImpl.create(result, record.asCountedStream());
  }
}
