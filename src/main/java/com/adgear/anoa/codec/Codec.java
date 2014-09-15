package com.adgear.anoa.codec;

import com.adgear.anoa.provider.Provider;

/**
 * Codecs are providers which transform records from another Provider, called the upstream Provider.
 * Similar in principle to the {@link com.google.common.base.Function} interface in Guava.
 *
 * @param <IN>  Type of the records consumed from the upstream Provider.
 * @param <OUT> Type of the record to be provided by the Codec.
 * @see com.adgear.anoa.provider.Provider
 * @see com.google.common.base.Function
 */
public interface Codec<IN, OUT> extends Provider<OUT> {

  /**
   * Method to be called by {@link Provider#next()}, but with guaranteed non-null argument.
   *
   * @param input The record to be transformed, provided by the upstream Provider, non-null.
   * @return The transformed record.
   */
  public OUT transform(final IN input);

  /**
   * Same method as in {@link com.adgear.anoa.provider.Provider}, but with generic types. Should not
   * return null.
   *
   * @return The upstream Provider.
   */
  public Provider<IN> getProvider();
}
