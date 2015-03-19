package com.adgear.anoa;

import java.io.IOException;

public class TestIOException extends IOException {

  final public long index;

  public TestIOException(long index) {
    super("read failed at index " + index);
    this.index = index;
  }
}
