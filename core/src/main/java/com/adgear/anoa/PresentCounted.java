package com.adgear.anoa;

public class PresentCounted implements AnoaCounted {

  private PresentCounted() {
  }

  static final PresentCounted INSTANCE = new PresentCounted();

  static public PresentCounted get() {
    return INSTANCE;
  }

  static public boolean is(AnoaCounted anoaCounted) {
    return (anoaCounted instanceof PresentCounted);
  }

  @Override
  public String toString() {
    return "Present";
  }
}
