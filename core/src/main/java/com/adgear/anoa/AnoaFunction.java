package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.function.Function;

public interface AnoaFunction<T, R>
    extends Function<@NonNull AnoaRecord<T>, @NonNull AnoaRecord<R>> {

  static <T, R> @NonNull AnoaFunction<T, R> of(@NonNull Function<T, R> function) {
    return new AnoaFunctionBase<T, R>() {
      @Override
      protected AnoaRecord<R> applyNonNull(@NonNull AnoaRecord<T> record) {
        return AnoaRecordImpl.create(function.apply(record.asOptional().get()),
                                     record.asCountedStream());
      }
    };
  }

  static <T, R> @NonNull AnoaFunction<T, R> pokemonize(@NonNull Function<T, R> function,
                                                       Class functionContext) {
    return new AnoaFunctionPokemon<>(function, functionContext);
  }

  interface ThrowingFunction<T, R> {
    R apply(T t) throws Exception;
  }

  static <T, R> @NonNull  AnoaFunction<T, R> pokemonizeChecked(
      @NonNull ThrowingFunction<T, R> function,
      Class functionContext) {
    return new AnoaFunctionPokemon<>(t -> {
      try {
        return function.apply(t);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, functionContext);
  }
}
