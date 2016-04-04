package com.adgear.anoa.test;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AnoaTestSampleTest {

  final AnoaTestSample s = new AnoaTestSample();

  @Test
  public void testFail() throws IOException {
    try {
      s.avroBinaryInputStream(500).skip(501);
    } catch (TestIOException e) {
      Assert.assertEquals(500, e.index);
    }
  }
}
