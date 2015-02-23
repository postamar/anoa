package com.adgear.anoa;

import checkers.nullness.quals.NonNull;

public interface AnoaCounted extends Comparable<AnoaCounted> {

  @Override
  default int compareTo(@NonNull AnoaCounted o) {
    return toString().compareTo(o.toString());
  }

  static AnoaCounted get(String label) {
    return AnoaCountedImpl.get(label);
  }

}
