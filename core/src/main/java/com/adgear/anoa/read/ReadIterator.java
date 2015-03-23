package com.adgear.anoa.read;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

final class ReadIterator<T> implements Iterator<T> {

  final protected Supplier<Boolean> eofClosure;
  final protected UnaryOperator<T> next;
  protected long counter = 0;

  ReadIterator(Supplier<Boolean> eofClosure,
               Function<Consumer<Boolean>, UnaryOperator<T>> nextFactory) {
    this.next = nextFactory.apply(this::setHasNext);
    this.eofClosure = eofClosure;
    reset();
  }

  private boolean isStale;
  private boolean hasNext;
  private T nextValue;

  void reset() {
    isStale = true;
    hasNext = true;
    nextValue = null;
  }

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
        ++counter;
        nextValue = next.apply(nextValue);
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