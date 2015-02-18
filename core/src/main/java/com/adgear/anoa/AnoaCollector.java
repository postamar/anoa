package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.stream.Collector;

public interface AnoaCollector<T, S extends AnoaSummary<T>>
    extends Collector<AnoaRecord<T>, S, AnoaSummary<T>> {

  static <T> @NonNull AnoaCollector<T, ? extends AnoaSummary<T>> inList() {
    return new AnoaCollectorImpl<>(() -> new AnoaSummaryImpl<>(new ArrayList<>(), false));
  }

  static <T> @NonNull AnoaCollector<T, ? extends AnoaSummary<T>> inSet() {
    return new AnoaCollectorImpl<>(() -> new AnoaSummaryImpl<>(new HashSet<>(), true));
  }

}
