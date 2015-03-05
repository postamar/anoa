package com.adgear.anoa.factory.util;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

abstract public class AbstractReadIterator<T> implements ReadIterator<T> {

  final protected Supplier<Boolean> eofClosure;

  protected AbstractReadIterator(Supplier<Boolean> eofClosure) {
    this.eofClosure = eofClosure;
  }

  private boolean isStale = true;
  private boolean hasNext = true;
  private T next = null;

  abstract protected T doNext();

  final protected void declareNoNext() {
    hasNext = false;
  }

  @Override
  final public boolean hasNext() {
    if (isStale) {
      isStale = false;
      if (eofClosure.get()) {
        declareNoNext();
      } else {
        next = doNext();
      }
    }
    return hasNext;
  }

  @Override
  final public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    isStale = true;
    return next;
  }
}