package com.adgear.anoa.provider;

import com.adgear.anoa.provider.base.CounterlessProviderBase;

/**
 * Provides at most one record when iterated over.
 *
 * @param <R> Type of the records to be provided.
 * @see com.adgear.anoa.provider.Provider
 */
public class SingleProvider<R> extends CounterlessProviderBase<R> {

  protected R datum = null;

  /**
   * Initialize with no records in queue.
   */
  public SingleProvider() {
    this(null);
  }

  /**
   * Initialize with one record in queue.
   */
  public SingleProvider(R datum) {
    this.datum = datum;
  }

  /**
   * Reset queue with datum. If null, then queue is emptied.
   */
  public void setDatum(R datum) {
    this.datum = datum;
  }

  @Override
  public boolean hasNext() {
    return (datum != null);
  }

  @Override
  public R getNext() {
    R result = datum;
    datum = null;
    return result;
  }
}
