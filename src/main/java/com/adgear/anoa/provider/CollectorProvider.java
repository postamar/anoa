package com.adgear.anoa.provider;

import com.adgear.anoa.provider.base.CounterlessProviderBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Provider implementation which acts as a priority queue. <p> A CollectorProvider is given a
 * collection of Providers. When iterating over a CollectorProvider, the 'best' next() record is
 * returned. Records are ranked according to a Comparator.
 *
 * @param <T> Type of the records to be provided.
 * @see com.adgear.anoa.provider.Provider
 * @see java.util.PriorityQueue
 */
public class CollectorProvider<T> extends CounterlessProviderBase<T> {

  final protected PriorityQueue<Wrapper<T>> queue;
  final protected List<Provider<T>> sources;

  /**
   * @param sources    The record Providers for the priority queue.
   * @param comparator Used for ordering the records at the head of the Providers.
   */
  public CollectorProvider(Collection<Provider<T>> sources, Comparator<T> comparator) {
    List<Wrapper<T>> wrapperList = new ArrayList<>(sources.size());
    for (Provider<T> source : sources) {
      wrapperList.add(new Wrapper<>(source, comparator));
    }
    this.queue = new PriorityQueue<>(wrapperList);
    this.sources = new ArrayList<>(sources);
  }

  @Override
  public boolean hasNext() {
    Wrapper<T> wrapper = queue.peek();
    return (wrapper != null) && wrapper.hasNext();
  }

  @Override
  protected T getNext() throws IOException {
    Wrapper<T> wrapper = queue.poll();
    T result = wrapper.next();
    queue.add(wrapper);
    return result;
  }

  /**
   * Aggregates the multiple source Providers into the top-level map.
   */
  @Override
  public Map<String, Map<String, Long>> getAllCounters() {
    Map<String, Map<String, Long>> map = super.getAllCounters();
    int i = 0;
    for (Provider<T> source : sources) {
      for (Map.Entry<String, Map<String, Long>> entry : source.getAllCounters().entrySet()) {
        map.put("[" + ++i + "] " + entry.getKey(), entry.getValue());
      }
    }
    return map;
  }

  /**
   * Resets all source Provider counters.
   */
  @Override
  public void resetAllCounters() {
    super.resetAllCounters();
    for (Provider<T> source : sources) {
      source.resetAllCounters();
    }
  }

  static class Wrapper<T> implements Comparable<Wrapper<T>>, Iterator<T> {

    protected Provider<T> source;
    protected T header;
    private Comparator<T> comparator;

    Wrapper(Provider<T> source, Comparator<T> comparator) {
      this.source = source;
      this.comparator = comparator;
      this.header = source.hasNext() ? source.next() : null;
    }

    public T next() {
      T result = header;
      if (header != null) {
        header = source.hasNext() ? source.next() : null;
      }
      return result;
    }

    @Override
    public boolean hasNext() {
      return (header != null);
    }

    @Override
    public void remove() {
      next();
    }

    @Override
    public int compareTo(Wrapper<T> o) {
      if (!hasNext()) {
        return 1;
      }
      if (!o.hasNext()) {
        return -1;
      }
      return comparator.compare(header, o.header);
    }
  }
}
