package com.adgear.anoa.tools.codec;

import com.adgear.anoa.DeserializerTest;
import com.adgear.anoa.codec.avro.JsonNodeToAvro;
import com.adgear.anoa.provider.SingleProvider;
import com.adgear.anoa.sink.CollectionSink;
import com.adgear.anoa.source.schemaless.JsonNodeSource;
import com.adgear.generated.avro.RecordNested;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import lombok.AllArgsConstructor;

public class CleanserTest {

  private Impression sampleImpression =
      new Impression("acme", new Conversion(12, new Date(1388530800L) /* 2014-01-01 */));
  private Impression sampleImpressionNullConversion =
      new Impression("acme", null);

  private Impression apply(String[] fields) {
    return new CleanserCodec<>(new SingleProvider<>(sampleImpression), fields).next();
  }

  @Test
  public void TestDoNothing() {
    Impression result = apply(new String[]{});
    Assert.assertTrue(result.impressorId != null);
    Assert.assertTrue(result.conversion != null);
    Assert.assertTrue(result.conversion.conversionDate != null);
  }

  @Test(expected = RuntimeException.class)
  public void TestAbsentField() {
    apply(new String[]{"absentField"});
  }

  @Test
  public void TestSingle() {
    Impression result = apply(new String[]{"impressorId"});
    Assert.assertTrue(result.impressorId == null);
    Assert.assertTrue(result.conversion != null);
    Assert.assertTrue(result.conversion.conversionDate != null);
  }

  @Test
  public void TestSimple() {
    Impression result = apply(new String[]{"impressorId", "conversion"});
    Assert.assertTrue(result.impressorId == null);
    Assert.assertTrue(result.conversion == null);
  }

  @Test
  public void TestNested() {
    Impression result = apply(new String[]{"conversion.conversionDate"});
    Assert.assertTrue(result.impressorId != null);
    Assert.assertTrue(result.conversion != null);
    Assert.assertTrue(result.conversion.conversionDate == null);
  }

  @Test(expected = RuntimeException.class)
  public void TestAbsentNested() {
    Impression result = apply(new String[]{"conversion.absentField"});
  }

  @Test(expected = RuntimeException.class)
  public void TestBadPrimitiveNested() {
    // conversionId does not have an object type, and therefore cannot be set to null.
    Impression result = apply(new String[]{"conversion.conversionId"});
  }

  @Test
  public void TestNoBorkOnNull() {
    /* In this example, asking to clear `conversion.conversionDate' when `conversion'
       is null should not trigger an error, it should be a silent no-op. */
    Impression result = new CleanserCodec<>(new SingleProvider<>(sampleImpressionNullConversion),
                                            new String[]{"conversion.conversionDate"}).next();
    Assert.assertTrue(result.impressorId != null);
    Assert.assertTrue(result.conversion == null);
  }

  @Test
  public void TestMany() throws IOException {
    List<RecordNested> list = new CollectionSink<>(new ArrayList<RecordNested>())
        .appendAll(
            new CleanserCodec<>(
                new JsonNodeToAvro<>(
                    new JsonNodeSource(DeserializerTest.streamResource("/multirecord.json")),
                    RecordNested.class),
                new String[]{"count", "nested.names"}))
        .getCollection();

    Assert.assertEquals(13, list.size());

    for (RecordNested e : list) {
      Assert.assertNull(e.getCount());
      if (e.getNested() != null) {
        Assert.assertNull(e.getNested().getNames());
      }
    }
  }

  @AllArgsConstructor
  public class Conversion {

    public int conversionId;
    public Date conversionDate;
  }

  @AllArgsConstructor
  public class Impression implements GenericContainer {

    public String impressorId;
    public Conversion conversion;

    @Override
    public Schema getSchema() {
      return null;
    } // Good enough for testing.
  }

}
