package com.adgear.anoa.sink;

import com.adgear.anoa.provider.Provider;

import java.util.Collection;

/**
 * Collects records by adding them to a provided Collection object.
 *
 * @param <T> The type of the records consumed by the Sink.
 * @param <C> The type of the Collection into which the records are added.
 * @see com.adgear.anoa.sink.Sink
 */
public class CollectionSink<T, C extends Collection<T>> implements Sink<T, CollectionSink<T, C>> {

  final private C collection;

  /**
   * @param collection The Collection object which is wrapped by this Sink.
   */
  public CollectionSink(C collection) {
    this.collection = collection;
  }

  @Override
  public CollectionSink<T, C> append(T record) {
    if (record != null) {
      collection.add(record);
    }
    return this;
  }

  @Override
  public CollectionSink<T, C> appendAll(Provider<T> provider) {
    for (T element : provider) {
      append(element);
    }
    return this;
  }

  @Override
  public void close() {
  }

  @Override
  public void flush() {
  }

  /**
   * @return The Collection object wrapped by this Sink.
   */
  public C getCollection() {
    return collection;
  }
}
