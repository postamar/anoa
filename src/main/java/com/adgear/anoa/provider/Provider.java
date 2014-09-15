package com.adgear.anoa.provider;

import java.util.Iterator;
import java.util.Map;

/**
 * Extends Iterable and Iterator with counters linked to {@link #next()}. In practice, a Provider
 * should be either a {@link com.adgear.anoa.source.Source} or a {@link
 * com.adgear.anoa.codec.Codec}, depending on whether the provided records originate internally or
 * from an 'upstream' Provider, exposed through {@link #getProvider()}. Similar in principle to the
 * {@link com.google.common.base.Supplier} interface in Guava.
 *
 * @param <T> Type of the records to be provided.
 * @see com.adgear.anoa.codec.Codec
 * @see com.adgear.anoa.source.Source
 * @see com.google.common.base.Supplier
 */
public interface Provider<T> extends Iterable<T>, Iterator<T> {

  /**
   * @return Number of records provided in total.
   */
  public long getCountTotal();

  /**
   * @return Number of occurences where {@link Provider#next()} returns null.
   */
  public long getCountDropped();

  /**
   * @return A map of counters of records processed.
   */
  public Map<String, Long> getCounters();

  /**
   * Resets all stored counters to zero.
   */
  public void resetCounters();

  /**
   * @return Another provider, in case the provider implementation itself consumes from another
   * provider to produce records, as is the case for {@link com.adgear.anoa.codec.Codec}
   * implementations.
   */
  public Provider<?> getProvider();

  /**
   * @return A map of all counters, aggregated by iterating over {@link #getProvider()}.
   */
  public Map<String, Map<String, Long>> getAllCounters();

  /**
   * Calls {@link #resetCounters()} iterating over {@link #getProvider()}.
   */
  public void resetAllCounters();
}
