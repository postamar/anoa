package com.adgear.anoa.read;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

final class ReadIterator<T> implements Iterator<T> {

  final protected Supplier<Boolean> eofClosure;
  final protected Supplier<T> next;

  ReadIterator(Function<Consumer<Boolean>, Supplier<T>> nextFactory, Supplier<Boolean> eofClosure) {
    this.next = nextFactory.apply(this::setHasNext);
    this.eofClosure = eofClosure;
  }

  private boolean isStale = true;
  private boolean hasNext = true;
  private T nextValue = null;

  protected void setHasNext(boolean hasNext) {
    this.hasNext = hasNext;
  }

  @Override
  public boolean hasNext() {
    if (isStale) {
      isStale = false;
      if (eofClosure.get()) {
        setHasNext(false);
      } else {
        nextValue = next.get();
      }
    }
    return hasNext;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    isStale = true;
    return nextValue;
  }

  public Stream<T> stream() {
    return ReadIteratorUtils.stream(this);
  }
}