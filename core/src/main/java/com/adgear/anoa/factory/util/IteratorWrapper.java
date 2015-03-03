package com.adgear.anoa.factory.util;

import java.util.Iterator;

/**
* Created by postamar on 15-02-27.
*/
public class IteratorWrapper<T> implements ReadIterator<T> {

  final private Iterator<T> iterator;

  public IteratorWrapper(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }
}
