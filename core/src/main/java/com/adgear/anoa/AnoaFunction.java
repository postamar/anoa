package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.impl.AnoaFunctionBase;
import com.adgear.anoa.impl.AnoaFunctionPokemon;
import com.adgear.anoa.impl.AnoaRecordImpl;

import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedFunction;

import java.util.function.Function;

public interface AnoaFunction<T, R>
    extends Function<@NonNull AnoaRecord<T>, @NonNull AnoaRecord<R>> {

  static <T, R> @NonNull AnoaFunction<T, R> of(@NonNull Function<T, R> function) {
    return new AnoaFunctionBase<T, R>() {
      @Override
      protected AnoaRecord<R> applyPresent(@NonNull AnoaRecord<T> record) {
        return AnoaRecordImpl.create(function.apply(record.asOptional().get()),
                                     record.asCountedStream());
      }
    };
  }

  static <T, R> @NonNull AnoaFunction<T, R> pokemon(@NonNull Function<T, R> function,
                                                    Class functionContext) {
    return new AnoaFunctionPokemon<>(function, functionContext);
  }

  static <T, R> @NonNull  AnoaFunction<T, R> pokemonChecked(
      @NonNull CheckedFunction<T, R> function,
      Class functionContext) {
    return pokemon(Unchecked.function(function), functionContext);
  }
}
