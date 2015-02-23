package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.Optional;
import java.util.stream.Stream;

public interface AnoaRecord<T> {

  @NonNull Optional<T> asOptional();

  default @NonNull Stream<@NonNull AnoaCounted> asCountedStream() {
    return Stream.of(asOptional().isPresent() ? PresentCounted.INSTANCE : EmptyCounted.INSTANCE);
  }

  default @NonNull Stream<@NonNull T> asStream() {
    return asOptional().isPresent() ? Stream.of(asOptional().get()) : Stream.<T>empty();
  }

  static <T> @NonNull AnoaRecord<T> of(T record) {
    return AnoaRecordImpl.create(record);
  }
}
