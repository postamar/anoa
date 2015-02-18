package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.function.Function;
import java.util.stream.Stream;

public interface AnoaFunction<T, R> extends Function<AnoaRecord<T>, AnoaRecord<R>> {

  @Override
  @NonNull AnoaRecord<R> apply(@NonNull AnoaRecord<T> record);

  static <T, R> AnoaFunction<T, R> of(@NonNull Function<T, R> function) {
    return new AnoaFunctionBase<T, R>() {
      @Override
      protected AnoaRecord<R> applyNonNull(@NonNull T record,
                                           @NonNull Stream<AnoaCounted> countedStream) {
        return AnoaRecordImpl.create(function.apply(record), countedStream);
      }
    };
  }

  static <T, R> AnoaFunction<T, R> pokemonize(@NonNull ThrowingFunction<T, R> function) {
    return pokemonize(function, null);
  }

  static <T, R> AnoaFunction<T, R> pokemonize(@NonNull ThrowingFunction<T, R> function,
                                              Class functionContext) {
    return new AnoaFunctionPokemon<>(function, functionContext);
  }
}
