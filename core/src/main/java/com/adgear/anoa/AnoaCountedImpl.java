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

  static AnoaCountedImpl get(String label) {
    return cache.computeIfAbsent(label, AnoaCountedImpl::new);
  }

  static final class NullStatus extends AnoaCountedImpl {

    private NullStatus(String label) {
      super(label);
    }

    static NullStatus getNullStatus(Object o) {
      return (o == null) ? MISSING : PRESENT;
    }

    static final NullStatus PRESENT = new NullStatus("PRESENT");
    static final NullStatus MISSING = new NullStatus("MISSING");
  }

  static private ConcurrentHashMap<String, AnoaCountedImpl> cache = new ConcurrentHashMap<>();
}
