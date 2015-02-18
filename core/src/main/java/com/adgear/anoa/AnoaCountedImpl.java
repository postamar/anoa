package com.adgear.anoa;

import java.util.concurrent.ConcurrentHashMap;

class AnoaCountedImpl implements AnoaCounted {

  final public String label;

  AnoaCountedImpl(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return label;
  }

  static AnoaCounted getPresentOrNotPresent(Object o) {
    return (o == null) ? NOT_PRESENT : PRESENT;
  }

  static final Basic PRESENT = new Basic("PRESENT");
  static final Basic NOT_PRESENT = new Basic("NOT_PRESENT");
  static final Basic UNSPECIFIED = new Basic("UNSPECIFIED");

  static final class Basic extends AnoaCountedImpl {

    private Basic(String label) {
      super(label);
    }

  }

  static private ConcurrentHashMap<String, AnoaCountedImpl> cache = new ConcurrentHashMap<>();

  static AnoaCountedImpl get(String label) {
    return cache.computeIfAbsent(label, AnoaCountedImpl::new);
  }
}
