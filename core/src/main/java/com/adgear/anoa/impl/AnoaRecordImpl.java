package com.adgear.anoa.impl;

import checkers.nullness.quals.NonNull;
import checkers.nullness.quals.Nullable;

import com.adgear.anoa.AnoaCounted;
import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.EmptyCounted;
import com.adgear.anoa.PresentCounted;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnoaRecordImpl<T> implements AnoaRecord<T> {

  final Optional<T> optional;
  final private List<@NonNull AnoaCounted> countedList;

  static public <T> AnoaRecordImpl<T> copy(@NonNull AnoaRecordImpl<T> other) {
    return new AnoaRecordImpl<>(other.optional, other.countedList);
  }

  static public <T> AnoaRecordImpl<T> create(@Nullable T record) {
    return new AnoaRecordImpl<>(Optional.ofNullable(record), null);
  }

  static public <T> AnoaRecordImpl<T> create(T record,
                                      Stream<@NonNull AnoaCounted> countedStream) {
    final List<AnoaCounted> list = countedStream
        .filter(c -> !(c instanceof PresentCounted) && !(c instanceof EmptyCounted))
        .collect(Collectors.toList());
    return new AnoaRecordImpl<>(Optional.ofNullable(record), list.isEmpty() ? null : list);
  }

  static public <T> AnoaRecordImpl<T> createEmpty(AnoaRecord<?> other,
                                           Stream<@NonNull AnoaCounted> extraCountedStream) {
    final List<AnoaCounted> list = Stream.concat(
        extraCountedStream,
        other.asCountedStream()
            .filter(c -> !(c instanceof PresentCounted) && !(c instanceof EmptyCounted)))
        .collect(Collectors.toList());
    return new AnoaRecordImpl<>(Optional.empty(), list.isEmpty() ? null : list);
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
