package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.impl.AnoaFunctionBase;
import com.adgear.anoa.impl.AnoaFunctionPokemon;

import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedConsumer;

import java.util.function.Consumer;

public interface AnoaConsumer<T> extends Consumer<@NonNull AnoaRecord<T>> {

  static <T> @NonNull AnoaConsumer<T> of(@NonNull Consumer<T> consumer) {
    return new AnoaFunctionBase<T, T>() {
      @Override
      protected AnoaRecord<T> applyPresent(@NonNull AnoaRecord<@NonNull T> record) {
        consumer.accept(record.asOptional().get());
        return record;
      }
    };
  }

  static <T> @NonNull AnoaConsumer<T> pokemonize(@NonNull Consumer<T> consumer,
                                                 Class functionContext) {
    return new AnoaFunctionPokemon<>(t -> {consumer.accept(t); return t;}, functionContext);
  }

  static <T, R> @NonNull AnoaConsumer<T> pokemonizeChecked(@NonNull CheckedConsumer<T> consumer,
                                                            Class functionContext) {
    return pokemonize(Unchecked.consumer(consumer), functionContext);
  }
}
