package com.adgear.anoa;

import org.junit.Assert;
import org.junit.Test;

public class AnoaReflectionUtilsTest {

  @Test
  public void testAvro() throws Exception {
    Assert.assertNotNull(AnoaReflectionUtils.getAvroClass("open_rtb.BidRequestAvro"));
  }

  @Test
  public void testThrift() throws Exception {
    Assert.assertEquals(14, AnoaReflectionUtils.getThriftMetaDataMap(
        AnoaReflectionUtils.getThriftClass("open_rtb.BidRequestThrift"))
        .entrySet().stream()
        .count());
  }

}
