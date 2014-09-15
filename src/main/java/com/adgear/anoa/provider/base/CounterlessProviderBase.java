package com.adgear.anoa.provider.base;

/**
 * Convenience base class for Providers with no specific counters.
 *
 * @param <T> Type of the records to be provided.
 * @see com.adgear.anoa.provider.base.ProviderBase
 */
abstract public class CounterlessProviderBase<T>
    extends ProviderBase<T, CounterlessProviderBase.Counter> {

  public CounterlessProviderBase() {
    super(Counter.class);
  }

  /**
   * Empty enum.
   */
  static public enum Counter {
  }
}
