package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface AnoaRecord<T> extends Supplier<T> {

  default boolean isPresent() {
    return (get() != null);
  }

  default @NonNull Stream<@NonNull AnoaCounted> asCountedStream() {
    return Stream.of(AnoaCountedImpl.NullStatus.getNullStatus(get()));
  }

  default @NonNull Stream<@NonNull T> asStream() {
    return isPresent() ? Stream.of(get()) : Stream.<T>empty();
  }

  default @NonNull Optional<T> asOptional() {
    return isPresent() ? Optional.of(get()) : Optional.<T>empty();
  }

  static <T> @NonNull AnoaRecord<T> of(T record) {
    return AnoaRecordImpl.create(record);
  }
}
