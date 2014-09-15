package com.adgear.anoa.tools.codec;

import com.adgear.anoa.provider.SingleProvider;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

import lombok.AllArgsConstructor;

public class FilterTest {

  private Impression sampleImpression =
      new Impression("acme", new Conversion(12, new Date(1388530800L) /* 2014-01-01 */));

  private boolean matchesSampleImpression(String whereClause) {
    return null != new FilterCodec<>(new SingleProvider<>(sampleImpression), Impression.class,
                                     whereClause).next();
  }

  @Test
  public void TestTrivialTrue() {
    Assert.assertTrue(matchesSampleImpression("TRUE"));
  }

  @Test
  public void TestTrivialFalse() {
    Assert.assertFalse(matchesSampleImpression("FALSE"));
  }

  @Test
  public void TestSimple() {
    Assert.assertTrue(matchesSampleImpression("impressorId = 'acme'"));
  }

  @Test
  public void TestNestedTrue() {
    Assert.assertTrue(matchesSampleImpression(
        "conversion.conversionId > 6 AND conversion.conversionDate > 2013-07-31"));
  }

  @Test
  public void TestNestedFalse() {
    Assert.assertTrue(matchesSampleImpression(
        "conversion.conversionId > 6 AND conversion.conversionDate > 2014-07-31"));
  }

  @AllArgsConstructor
  public class Conversion {

    public int conversionId;
    public Date conversionDate;
  }

  @AllArgsConstructor
  public class Impression {

    public String impressorId;
    public Conversion conversion;
  }
}
