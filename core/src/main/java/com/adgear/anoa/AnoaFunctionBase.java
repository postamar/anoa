package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.stream.Stream;

abstract public class AnoaFunctionBase<T, R> implements AnoaFunction<T, R> {

  abstract protected @NonNull AnoaRecord<R> applyNonNull(
      @NonNull T record,
      @NonNull Stream<AnoaCounted> countedStream);

  @Override
  @SuppressWarnings("unchecked")
  public @NonNull AnoaRecord<R> apply(@NonNull AnoaRecord<T> record) {
    final T wrapped = record.get();
    return (wrapped == null)
           ? ((record instanceof AnoaRecordImpl)
              ? (AnoaRecordImpl<R>) record
              : AnoaRecordImpl.create(null, record.asCountedStream()))
           : applyNonNull(wrapped, record.asCountedStream());
  }
}
