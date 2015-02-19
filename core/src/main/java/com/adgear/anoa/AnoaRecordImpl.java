package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class AnoaRecordImpl<T> implements AnoaRecord<T> {

  final T record;
  final private List<@NonNull AnoaCounted> countedList;

  static <T> AnoaRecordImpl<T> copy(AnoaRecordImpl<T> other) {
    return new AnoaRecordImpl<>(other.record, other.countedList);
  }

  static <T> AnoaRecordImpl<T> create(T record) {
    return new AnoaRecordImpl<>(record, null);
  }

  static <T> AnoaRecordImpl<T> create(T record, Stream<@NonNull AnoaCounted> countedStream) {
    final List<AnoaCounted> list = countedStream
        .filter(c -> !(c instanceof AnoaCountedImpl.NullStatus))
        .collect(Collectors.toList());
    return new AnoaRecordImpl<>(record, list.isEmpty() ? null : list);
  }

  private AnoaRecordImpl(T record, List<AnoaCounted> countedList) {
    this.record = record;
    this.countedList = countedList;
  }

  @Override
  public T get() {
    return record;
  }

  @Override
  public boolean isPresent() {
    return (record != null);
  }

  @Override
  public Stream<AnoaCounted> asCountedStream() {
    return (countedList == null)
           ? AnoaRecord.super.asCountedStream()
           : Stream.concat(AnoaRecord.super.asCountedStream(), countedList.stream());
  }

  @Override
  public String toString() {
    return (record == null)
           ? asCountedStream().map(Object::toString).collect(missingCollector)
           : asStream().map(Object::toString).collect(presentCollector);
  }

  static private Collector<CharSequence,?,String> presentCollector =
      Collectors.joining(", ", "AnoaRecord(", ")");

  static private Collector<CharSequence,?,String> missingCollector =
      Collectors.joining(", ", "AnoaRecord<", ">");
}
