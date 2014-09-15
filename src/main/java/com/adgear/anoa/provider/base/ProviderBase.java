package com.adgear.anoa.provider.base;

import com.adgear.anoa.provider.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for most {@link com.adgear.anoa.provider.Provider} implementations.
 *
 * @param <T> Type of the records to be provided.
 * @param <C> Counter class, usually an enum named <code>Counter</code> declared as subclass.
 *            Represents available keys for map returned by {@link #getCounters()}.
 * @see com.adgear.anoa.provider.Provider
 */
abstract public class ProviderBase<T, C extends Enum<C>> implements Provider<T> {

  final protected Logger logger;

  private long total;
  private long dropped;
  private EnumMap<C, Long> counters;

  protected ProviderBase(Class<C> counterEnumClass) {
    this.logger = LoggerFactory.getLogger(this.getClass());
    this.counters = new EnumMap<>(counterEnumClass);
    resetCounters();
  }

  abstract protected T getNext() throws IOException;

  /**
   * Wraps abstract protected method {@link #getNext()} in counter logic and exception handling.
   *
   * @return the next provided record, or null if dropped.
   */
  @Override
  public T next() {
    ++total;
    T nextValue = null;
    try {
      nextValue = getNext();
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    if (nextValue == null) {
      ++dropped;
    }
    return nextValue;
  }

  @Override
  public void remove() {
    throw new RuntimeException("remove() method not supported.");
  }

  /**
   * @return this
   */
  @Override
  public Iterator<T> iterator() {
    return this;
  }

  @Override
  public long getCountTotal() {
    return total;
  }

  @Override
  public long getCountDropped() {
    return dropped;
  }

  @Override
  public Map<String, Long> getCounters() {
    Map<String, Long> map = new LinkedHashMap<>();
    for (Map.Entry<C, Long> entry : counters.entrySet()) {
      map.put(entry.getKey().toString(), entry.getValue());
    }
    return map;
  }

  @Override
  public Map<String, Map<String, Long>> getAllCounters() {
    Map<String, Map<String, Long>> map = new LinkedHashMap<>();
    for (Provider<?> source = this; source != null; source = source.getProvider()) {
      map.put(source.getClass().getCanonicalName(), getCounters());
    }
    return map;
  }

  @Override
  public Provider<?> getProvider() {
    return null;
  }

  @Override
  public void resetCounters() {
    total = 0L;
    dropped = 0L;
    counters.clear();
  }

  @Override
  public void resetAllCounters() {
    for (Provider<?> source = this; source != null; source = source.getProvider()) {
      source.resetCounters();
    }
  }

  /**
   * @param counter Token of counter to be incremented.
   */
  final protected void increment(C counter) {
    Long value = counters.get(counter);
    counters.put(counter, ((value == null) ? 0L : value) + 1L);
  }

}
