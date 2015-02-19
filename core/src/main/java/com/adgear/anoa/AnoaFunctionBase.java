package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

abstract class AnoaFunctionBase<T, R> implements AnoaFunction<T, R>, AnoaConsumer<T> {

  abstract protected AnoaRecord<R> applyNonNull(@NonNull AnoaRecord<@NonNull T> record);

  protected void acceptNonNull(@NonNull AnoaRecord<@NonNull T> record) {
    applyNonNull(record);
  }

  @Override
  @SuppressWarnings("unchecked")
  public @NonNull AnoaRecord<R> apply(@NonNull AnoaRecord<T> record) {
    return record.isPresent()
           ? applyNonNull(record)
           : ((record instanceof AnoaRecordImpl)
              ? (AnoaRecordImpl<R>) record
              : AnoaRecordImpl.create(null, record.asCountedStream()));
  }

  @Override
  public void accept(@NonNull AnoaRecord<T> record) {
    if (record.isPresent()) {
      acceptNonNull(record);
    }
  }
}
