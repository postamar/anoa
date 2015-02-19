package com.adgear.anoa;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class AnoaCollectorImpl<T> implements AnoaCollector<T, AnoaSummaryImpl<T>> {

  final Supplier<AnoaSummaryImpl<T>> supplier;
  final Set<Characteristics> characteristicsSet;

  AnoaCollectorImpl(Supplier<AnoaSummaryImpl<T>> supplier) {
    this.supplier = supplier;
    this.characteristicsSet = supplier.get().isConcurrent
                              ? CONCURRENT_CHARACTERISTICS
                              : SEQUENTIAL_CHARACTERISTICS;
  }

  @Override
  public Supplier<AnoaSummaryImpl<T>> supplier() {
    return supplier;
  }

  @Override
  public BiConsumer<AnoaSummaryImpl<T>, AnoaRecord<T>> accumulator() {
    return supplier.get().isConcurrent
           ? AnoaSummaryImpl::concurrentAccumulate
           : AnoaSummaryImpl::accumulate;
  }

  @Override
  public BinaryOperator<AnoaSummaryImpl<T>> combiner() {
    return AnoaSummaryImpl::combine;
  }

  @Override
  public Function<AnoaSummaryImpl<T>, AnoaSummary<T>> finisher() {
    return AnoaCollectorImpl::finisher;
  }

  @Override
  public Set<Characteristics> characteristics() {
    return characteristicsSet;
  }

  static private <T> AnoaSummary<T> finisher(AnoaSummaryImpl<T> anoaSummary) {
    return anoaSummary;
  }

  static Set<Characteristics> CONCURRENT_CHARACTERISTICS = Collections.unmodifiableSet(
      Stream
          .of(Characteristics.IDENTITY_FINISH, Characteristics.CONCURRENT)
          .collect(Collectors.toSet()));

  static Set<Characteristics> SEQUENTIAL_CHARACTERISTICS = Collections.unmodifiableSet(
      Stream
          .of(Characteristics.IDENTITY_FINISH)
          .collect(Collectors.toSet()));
}
