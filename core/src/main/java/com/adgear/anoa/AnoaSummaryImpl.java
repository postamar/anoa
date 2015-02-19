package com.adgear.anoa;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

class AnoaSummaryImpl<T> implements AnoaSummary<T> {

  final private Collection<T> present;
  final private Map<AnoaCounted,Long> counters;
  final boolean isConcurrent;

  AnoaSummaryImpl(Collection<T> present, boolean isConcurrent) {
    this.present = present;
    this.counters = new HashMap<>();
    this.isConcurrent = isConcurrent;
  }

  synchronized void concurrentAccumulate(AnoaRecord<T> record) {
    accumulate(record);
  }

  void accumulate(AnoaRecord<T> record) {
    if (present != null && record.asOptional().isPresent()) {
      present.add(record.asOptional().get());
    }
    record.asCountedStream()
        .forEach(ac -> counters.merge(ac, 1L, Long::sum));
  }

  synchronized AnoaSummaryImpl<T> combine(AnoaSummaryImpl<T> otherSummary){
    present.addAll(otherSummary.present);
    otherSummary.counters.entrySet().stream()
        .forEach(e -> counters.merge(e.getKey(), e.getValue(), Long::sum));
    return this;
  }

  @Override
  public Stream<T> streamPresent() {
    return present.stream();
  }

  @Override
  public Stream<Map.Entry<AnoaCounted, Long>> streamCounters() {
    return counters.entrySet().stream();
  }

  @Override
  public Iterator<T> iterator() {
    return present.iterator();
  }
}
