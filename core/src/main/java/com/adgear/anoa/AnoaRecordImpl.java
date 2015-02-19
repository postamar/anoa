package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class AnoaRecordImpl<T> implements AnoaRecord<T> {

  final Optional<T> optional;
  final private List<@NonNull AnoaCounted> countedList;

  static <T> AnoaRecordImpl<T> copy(AnoaRecordImpl<T> other) {
    return new AnoaRecordImpl<>(other.optional, other.countedList);
  }

  static <T> AnoaRecordImpl<T> create(T record) {
    return new AnoaRecordImpl<>(Optional.ofNullable(record), null);
  }

  static <T> AnoaRecordImpl<T> create(T record, Stream<@NonNull AnoaCounted> countedStream) {
    final List<AnoaCounted> list = countedStream
        .filter(c -> !(c instanceof AnoaCountedImpl.NullStatus))
        .collect(Collectors.toList());
    return new AnoaRecordImpl<>(Optional.ofNullable(record), list.isEmpty() ? null : list);
  }

  private AnoaRecordImpl(Optional<T> optional, List<AnoaCounted> countedList) {
    this.optional = optional;
    this.countedList = countedList;
  }

  @Override
  public Optional<T> asOptional() {
    return optional;
  }

  @Override
  public Stream<AnoaCounted> asCountedStream() {
    return (countedList == null)
           ? AnoaRecord.super.asCountedStream()
           : Stream.concat(AnoaRecord.super.asCountedStream(), countedList.stream());
  }

  @Override
  public String toString() {
    return optional.isPresent()
           ? asStream().map(Object::toString).collect(presentCollector)
           : asCountedStream().map(Object::toString).collect(missingCollector);

  }

  static private Collector<CharSequence,?,String> presentCollector =
      Collectors.joining(", ", "AnoaRecord(", ")");

  static private Collector<CharSequence,?,String> missingCollector =
      Collectors.joining(", ", "AnoaRecord<", ">");
}
