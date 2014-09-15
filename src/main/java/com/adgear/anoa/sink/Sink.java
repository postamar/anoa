package com.adgear.anoa.sink;

import com.adgear.anoa.provider.Provider;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * Sinks are collectors for consuming records from Providers.
 *
 * @param <T> The type of the records consumed by the Sink.
 * @param <S> The type of the Sink itself, used for fluency.
 * @see com.adgear.anoa.provider.Provider
 */
public interface Sink<T, S extends Sink<T, S>> extends Closeable, Flushable {

  /**
   * Appends a record into the sink. Does nothing if null.
   *
   * @param record A record to consume. May be null.
   * @return this, for fluency.
   */
  public S append(T record) throws IOException;

  /**
   * Iterates over provider and applies {@link #append(Object)} to each non-null record. Concludes
   * by a call to {@link #flush()}
   *
   * @param provider The provider from which to consume from.
   * @return this, for fluency.
   */
  public S appendAll(Provider<T> provider) throws IOException;
}
