package com.adgear.anoa;

import java.util.Objects;
import java.util.function.Function;

/**
 * Functional interface for exception handlers with no extra arguments
 *
 * @param <M> Metadata type
 */
public interface Handler0<M> extends Function<Throwable, M[]> {

  /**
   * Convenience factory method for transforming handlers which map an exception to a single
   * meta-datum.
   *
   * @param mapToMetaDatum exception handler
   * @param <M> Metadata type
   * @return an instance wrapping the result of {@code mapToMetaDatum} in an array.
   */
  static <M> /*@NonNull*/ Handler0<M> of(
      /*@NonNull*/ Function<Throwable, M> mapToMetaDatum) {
    Objects.requireNonNull(mapToMetaDatum);
    return t -> AnoaHandler.arraySize1(mapToMetaDatum.apply(t));
  }
}
