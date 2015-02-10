package com.adgear.anoa.provider;

import com.adgear.anoa.provider.base.CounterlessProviderBase;

import java.io.IOException;
import java.util.Iterator;


public class InfiniteLoopProvider<R> extends CounterlessProviderBase<R> {

  final protected Iterable<R> iterable;
  private Iterator<R> iterator;

  public InfiniteLoopProvider(Iterable<R> iterable) {
    this.iterable = iterable;
    iterator = iterable.iterator();
  }

  @Override
  protected R getNext() throws IOException {
    if (!iterator.hasNext()) {
      iterator = iterable.iterator();
    }
    return iterator.next();
  }

  @Override
  public boolean hasNext() {
    return true;
  }
}