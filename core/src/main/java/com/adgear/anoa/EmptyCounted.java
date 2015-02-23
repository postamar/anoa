package com.adgear.anoa;


public class EmptyCounted implements AnoaCounted {

  private EmptyCounted() {
  }

  static final EmptyCounted INSTANCE = new EmptyCounted();

  static public EmptyCounted get() {
    return INSTANCE;
  }

  static public boolean is(AnoaCounted anoaCounted) {
    return (anoaCounted instanceof EmptyCounted);
  }

  @Override
  public String toString() {
    return "Empty";
  }
}
