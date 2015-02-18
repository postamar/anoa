package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

import java.util.Map.Entry;
import java.util.stream.Stream;

public interface AnoaSummary<T> extends Iterable<T> {

  @NonNull Stream<T> streamPresent();

  @NonNull Stream<Entry<@NonNull AnoaCounted, @NonNull Long>> streamCounters();
}
