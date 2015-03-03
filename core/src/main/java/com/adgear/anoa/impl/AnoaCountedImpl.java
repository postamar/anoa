package com.adgear.anoa.impl;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaCounted;

import java.util.concurrent.ConcurrentHashMap;

public class AnoaCountedImpl implements AnoaCounted {

  final @NonNull String label;

  AnoaCountedImpl(@NonNull String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return label;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null) {
      return false;
    } else if (getClass() != o.getClass()) {
      return false;
    } else {
      return label.equals(((AnoaCountedImpl) o).label);
    }
  }

  @Override
  public int hashCode() {
    return label.hashCode();
  }

  static public AnoaCountedImpl get(String label) {
    return cache.computeIfAbsent(label, AnoaCountedImpl::new);
  }

  static private ConcurrentHashMap<String, AnoaCountedImpl> cache = new ConcurrentHashMap<>();
}
