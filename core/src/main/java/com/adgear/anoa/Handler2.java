package com.adgear.anoa;

import java.util.Objects;


/**
 * Functional interface for exception handlers with two extra arguments
 *
 * @param <M> Metadata type
 */
public interface Handler2<M> {

  /**
   * Convenience factory method for transforming handlers which map an exception to a single
   * meta-datum.
   *
   * @param mapToMetaDatum exception handler
   * @param <M>            Metadata type
   * @return an instance wrapping the result of {@code mapToMetaDatum} in an array.
   */
  static <M> /*@NonNull*/ Handler2<M> of(
      /*@NonNull*/ TriFunction<Throwable, Object, Object, M> mapToMetaDatum) {
    Objects.requireNonNull(mapToMetaDatum);
    return (t, u, v) -> AnoaHandler.arraySize1(mapToMetaDatum.apply(t, u, v));
  }

  M[] apply(Throwable handled, Object value, Object other);

  /**
   * Functional interface for a function taking three arguments
   *
   * @param <T> Value type of argument 1
   * @param <U> Value type of argument 2
   * @param <V> Value type of argument 3
   * @param <R> Value type of result
   * @see java.util.function.BiFunction
   */
  interface TriFunction<T, U, V, R> {

    R apply(T handled, U value, V other);
  }

}
