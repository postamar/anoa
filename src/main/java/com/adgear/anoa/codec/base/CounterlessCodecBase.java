package com.adgear.anoa.codec.base;

import com.adgear.anoa.provider.Provider;

/**
 * Convenience base class for Codecs with no specific counters.
 *
 * @param <IN>  Type of the records consumed from the upstream Provider.
 * @param <OUT> Type of the record to be provided by the Codec.
 * @see com.adgear.anoa.codec.base.CodecBase
 */
abstract public class CounterlessCodecBase<IN, OUT>
    extends CodecBase<IN, OUT, CounterlessCodecBase.Counter> {

  protected CounterlessCodecBase(Provider<IN> provider) {
    super(provider, Counter.class);
  }

  /**
   * Empty enum.
   */
  static public enum Counter {
  }
}
