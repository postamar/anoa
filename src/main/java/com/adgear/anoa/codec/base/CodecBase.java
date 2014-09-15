package com.adgear.anoa.codec.base;

import com.adgear.anoa.codec.Codec;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.base.ProviderBase;

/**
 * Base class for most {@link com.adgear.anoa.codec.Codec} implementations.
 *
 * @param <IN>  Type of the records consumed from the upstream Provider.
 * @param <OUT> Type of the record to be provided by the Codec.
 * @param <C>   Counter class, usually an enum named <code>Counter</code> declared as subclass.
 *              Represents available keys for map returned by {@link #getCounters()}.
 * @see com.adgear.anoa.codec.Codec
 */
abstract public class CodecBase<IN, OUT, C extends Enum<C>>
    extends ProviderBase<OUT, C> implements Codec<IN, OUT> {

  private Provider<IN> provider;

  protected CodecBase(Provider<IN> provider, Class<C> counterEnumClass) {
    super(counterEnumClass);
    this.provider = provider;
  }

  @Override
  public boolean hasNext() {
    return (provider != null) && provider.hasNext();
  }

  @Override
  protected OUT getNext() {
    final IN input = provider.next();
    return (input == null) ? null : this.transform(input);
  }

  @Override
  public Provider<IN> getProvider() {
    return provider;
  }
}
