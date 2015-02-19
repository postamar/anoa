package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

public interface AnoaCounted extends Comparable<AnoaCounted> {

  default String getLabel() {
    return toString();
  }

  @Override
  default int compareTo(@NonNull AnoaCounted o) {
    return getLabel().compareTo(o.getLabel());
  }

  static AnoaCounted get(String label) {
    return AnoaCountedImpl.get(label);
  }

  static boolean isPresent(AnoaCounted anoaCounted) {
    return anoaCounted.equals(AnoaCountedImpl.NullStatus.PRESENT);
  }

  static boolean isMissing(AnoaCounted anoaCounted) {
    return anoaCounted.equals(AnoaCountedImpl.NullStatus.MISSING);
  }
}
