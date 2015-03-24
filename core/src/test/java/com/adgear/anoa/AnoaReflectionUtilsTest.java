package com.adgear.anoa;

import org.junit.Assert;
import org.junit.Test;

public class AnoaReflectionUtilsTest {

  @Test
  public void test() throws Exception {
    Assert.assertNotNull(AnoaReflectionUtils.getAvroClass("com.adgear.avro.Simple"));
    Assert.assertEquals(3, AnoaReflectionUtils.getThriftMetaDataMap(
        AnoaReflectionUtils.getThriftClass("thrift.com.adgear.avro.Simple"))
        .entrySet().stream()
        .count());
  }


}
