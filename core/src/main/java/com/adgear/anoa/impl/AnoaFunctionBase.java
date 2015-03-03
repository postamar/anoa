package com.adgear.anoa.impl;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaConsumer;
import com.adgear.anoa.AnoaFunction;
import com.adgear.anoa.AnoaRecord;

public abstract class AnoaFunctionBase<T, R> implements AnoaFunction<T, R>, AnoaConsumer<T> {

  abstract protected AnoaRecord<R> applyPresent(@NonNull AnoaRecord<@NonNull T> record);

  protected void acceptPresent(@NonNull AnoaRecord<@NonNull T> record) {
    applyPresent(record);
  }

  @Override
  @SuppressWarnings("unchecked")
  public @NonNull AnoaRecord<R> apply(@NonNull AnoaRecord<T> record) {
    return record.asOptional().isPresent()
           ? applyPresent(record)
           : ((record instanceof AnoaRecordImpl)
              ? (AnoaRecordImpl<R>) record
              : AnoaRecordImpl.create(null, record.asCountedStream()));
  }

  @Override
  public void accept(@NonNull AnoaRecord<T> record) {
    if (record.asOptional().isPresent()) {
      acceptPresent(record);
    }
  }
}
