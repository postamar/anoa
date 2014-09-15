package com.adgear.anoa.provider;

import com.adgear.anoa.provider.base.CounterlessProviderBase;

import java.io.IOException;
import java.util.Iterator;

/**
 * Provider implementation which wraps an Iterator.
 *
 * @param <T> Type of the records to be provided.
 * @see com.adgear.anoa.provider.Provider
 */
public class IteratorProvider<T> extends CounterlessProviderBase<T> {

  protected Iterator<T> iterator;

  /**
   * @param iterator the iterator instance to wrap
   */
  public IteratorProvider(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  @Override
  protected T getNext() throws IOException {
    return iterator.next();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }
}
