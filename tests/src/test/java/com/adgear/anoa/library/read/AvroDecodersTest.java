package com.adgear.anoa.library.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEvent;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
import com.adgear.anoa.test.ad_exchange.LogEventType;
import com.fasterxml.jackson.core.TreeNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroDecodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();


  @Test
  public void testBinary() {
    ATS.assertAvroGenerics(ATS.avroBinary().map(AvroDecoders.binary(ATS.avroSchema)));
    ATS.assertAvroSpecifics(ATS.avroBinary().map(AvroDecoders.binary(ATS.avroClass)));
  }

  @Test
  public void testJson() {
    ATS.assertAvroGenerics(ATS.avroJson().map(AvroDecoders.json(ATS.avroSchema)));
    ATS.assertAvroSpecifics(ATS.avroJson().map(AvroDecoders.json(ATS.avroClass,LogEventAvro::new)));
  }

  @Test
  public void testJackson() {
    ATS.assertAvroGenerics(
        ATS.jsonObjects()
            .map(TreeNode::traverse)
            .map(AvroDecoders.jacksonStrict(ATS.avroSchema)));

    ATS.assertAvroSpecifics(
        ATS.jsonObjects()
            .map(TreeNode::traverse)
            .map(AvroDecoders.jacksonStrict(ATS.avroClass)));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertAvroGenerics(
        ATS.avroBinary()
            .map(anoaHandler::<byte[]>of)
            .map(AvroDecoders.binary(anoaHandler, ATS.avroSchema))
            .map(Anoa::get));

    ATS.assertAvroSpecifics(
        ATS.avroBinary()
            .map(anoaHandler::<byte[]>of)
            .map(AvroDecoders.binary(anoaHandler, ATS.avroClass))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJson() {
    ATS.assertAvroGenerics(
        ATS.avroJson()
            .map(anoaHandler::<String>of)
            .map(AvroDecoders.json(anoaHandler, ATS.avroSchema))
            .map(Anoa::get));

    ATS.assertAvroSpecifics(
        ATS.avroJson()
            .map(anoaHandler::<String>of)
            .map(AvroDecoders.json(anoaHandler, ATS.avroClass))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJackson() {
    ATS.assertAvroGenerics(
        ATS.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(AvroDecoders.jackson(anoaHandler, ATS.avroSchema))
            .map(Anoa::get));

    ATS.assertAvroSpecifics(
        ATS.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(AvroDecoders.jackson(anoaHandler, ATS.avroClass))
            .map(Anoa::get));
  }

  @Test
  public void testJacksonStrictness() {
    LogEvent<?> strict = LogEvent.avro(AvroDecoders.jacksonStrict(ATS.avroClass)
                                           .apply(ATS.jsonNullsObjectParser()));
    LogEvent<?> loose = LogEvent.avro(AvroDecoders.jackson(ATS.avroClass)
                                          .apply(ATS.jsonNullsObjectParser()));
    Assert.assertNotNull(strict.getRequest());
    Assert.assertNotNull(strict.getResponse());
    Assert.assertNotNull(strict.getTimestamp());
    Assert.assertEquals(0L, strict.getTimestamp());
    Assert.assertNotNull(strict.getType());
    Assert.assertEquals(LogEventType.Avro.UNKNOWN_LOG_EVENT_TYPE, strict.getType());
    Assert.assertNotNull(strict.getUuid());
    Assert.assertEquals(16, strict.getUuid().get().length);
    Assert.assertNotNull(strict.getProperties());
    Assert.assertTrue(strict.getProperties().isEmpty());
    Assert.assertEquals(strict, loose);
  }

  @Test
  public void testAnoaBroken() {
    Map<String, List<Throwable>> metaMap = ATS.thriftCompact()
        .map(anoaHandler::<byte[]>of)
        .map(AvroDecoders.binary(anoaHandler, ATS.avroClass))
        .peek(a -> Assert.assertFalse(a.isPresent()))
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(t -> t.getClass().getSimpleName()));

    Assert.assertEquals(3, metaMap.size());
    Assert.assertNotNull(metaMap.get("EOFException"));
    Assert.assertNotNull(metaMap.get("AvroRuntimeException"));
    Assert.assertNotNull(metaMap.get("ArrayIndexOutOfBoundsException"));
    Assert.assertEquals(4, metaMap.get("EOFException").size());
    Assert.assertEquals(9, metaMap.get("AvroRuntimeException").size());
    Assert.assertEquals(987, metaMap.get("ArrayIndexOutOfBoundsException").size());
  }
}
