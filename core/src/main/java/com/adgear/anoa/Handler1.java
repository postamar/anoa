package com.adgear.anoa;

import java.util.Objects;
import java.util.function.BiFunction;


/**
 * Functional interface for exception handlers with one extra argument
 *
 * @param <M> Metadata type
 */
public interface Handler1<M> extends BiFunction<Throwable, Object, M[]> {

  /**
   * Convenience factory method for transforming handlers which map an exception to a single
   * meta-datum.
   *
   * @param mapToMetaDatum exception handler
   * @param <M>            Metadata type
   * @return an instance wrapping the result of {@code mapToMetaDatum} in an array.
   */
  static <M> /*@NonNull*/ Handler1<M> of(
      /*@NonNull*/ BiFunction<Throwable, Object, M> mapToMetaDatum) {
    Objects.requireNonNull(mapToMetaDatum);
    return (t, u) -> AnoaHandler.arraySize1(mapToMetaDatum.apply(t, u));
  }
}
