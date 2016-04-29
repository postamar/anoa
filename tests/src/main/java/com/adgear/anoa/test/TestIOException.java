package com.adgear.anoa.test;

import java.io.IOException;

public class TestIOException extends IOException {

  public final long index;

  public TestIOException(long index) {
    super("Exception raised when reading byte " + index);
    this.index = index;
  }
}
