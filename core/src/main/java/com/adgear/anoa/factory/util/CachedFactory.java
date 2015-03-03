package com.adgear.anoa.factory.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class CachedFactory<K,V> {

  final private ConcurrentHashMap<K,V> cache = new ConcurrentHashMap<>();
  final public Function<K,V> builderFn;

  public CachedFactory(Function<K, V> builderFn) {
    this.builderFn = builderFn;
  }

  public V get(K key) {
    return cache.computeIfAbsent(key, builderFn);
  }
}
