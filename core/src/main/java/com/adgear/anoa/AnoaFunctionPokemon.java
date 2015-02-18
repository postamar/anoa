package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.stream.Stream;

public class AnoaFunctionPokemon<T, R> extends AnoaFunctionBase<T, R> {

  final public ThrowingFunction<T, R> wrappedFunction;
  final public Class functionContext;

  AnoaFunctionPokemon(ThrowingFunction<T, R> wrappedFunction, Class functionContext) {
    this.wrappedFunction = wrappedFunction;
    this.functionContext = functionContext;
  }

  protected String toCountedLabel(Exception e) {
    return ((functionContext == null) ? "" : ("[ " + functionContext.getCanonicalName() + " ]\t"))
           + e.getMessage();
  }

  @Override
  final protected AnoaRecord<R> applyNonNull(@NonNull T record,
                                             @NonNull Stream<AnoaCounted> countedStream) {
    final R result;
    try {
      result = wrappedFunction.apply(record);
    } catch (Exception e) {
      final AnoaCounted counted = AnoaCounted.get(toCountedLabel(e));
      return AnoaRecordImpl.create(null, Stream.concat(Stream.of(counted), countedStream));
    }
    return AnoaRecordImpl.create(result, countedStream);
  }

}
