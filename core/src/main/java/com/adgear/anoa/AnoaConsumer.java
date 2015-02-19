package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.function.Consumer;

public interface AnoaConsumer<T> extends Consumer<@NonNull AnoaRecord<T>> {

  static <T> @NonNull AnoaConsumer<T> of(@NonNull Consumer<T> consumer) {
    return new AnoaFunctionBase<T, T>() {
      @Override
      protected AnoaRecord<T> applyNonNull(@NonNull AnoaRecord<@NonNull T> record) {
        consumer.accept(record.asOptional().get());
        return record;
      }
    };
  }

  static <T> @NonNull AnoaConsumer<T> pokemonize(@NonNull Consumer<T> consumer,
                                                 Class functionContext) {
    return new AnoaFunctionPokemon<>(t -> {consumer.accept(t); return t;}, functionContext);
  }

  interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
  }

  static <T, R> @NonNull AnoaConsumer<T> pokemonizeChecked(@NonNull ThrowingConsumer<T> consumer,
                                                            Class functionContext) {
    return new AnoaFunctionPokemon<>(t -> {
      try {
        consumer.accept(t);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return t;
    }, functionContext);
  }
}
