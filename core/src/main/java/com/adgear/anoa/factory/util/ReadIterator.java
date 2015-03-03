package com.adgear.anoa.factory.util;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ReadIterator<T> extends Iterator<T> {

  default public Spliterator<T> spliterator() {
    return Spliterators.spliteratorUnknownSize(this, Spliterator.NONNULL | Spliterator.ORDERED);
  }

  default public Stream<T> stream() {
    return StreamSupport.stream(spliterator(), false);
  }
}
