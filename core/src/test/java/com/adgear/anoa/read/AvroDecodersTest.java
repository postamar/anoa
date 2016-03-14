package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.avro.AvroRuntimeException;
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
    ATS.assertAvroGenerics(ATS.jsonObjects()
                                   .map(TreeNode::traverse)
                                   .map(AvroDecoders.jackson(ATS.avroSchema, true)));

    ATS.assertAvroSpecifics(ATS.jsonObjects()
                                    .map(TreeNode::traverse)
                                    .map(AvroDecoders.jackson(ATS.avroClass, true)));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertAvroGenerics(ATS.avroBinary()
                                   .map(anoaHandler::<byte[]>of)
                                   .map(AvroDecoders.binary(anoaHandler, ATS.avroSchema))
                                   .map(Anoa::get));

    ATS.assertAvroSpecifics(ATS.avroBinary()
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
            .map(AvroDecoders.jackson(anoaHandler, ATS.avroSchema, false))
            .map(Anoa::get));

    ATS.assertAvroSpecifics(
        ATS.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(AvroDecoders.jackson(anoaHandler, ATS.avroClass, false))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaBroken() {
    Map<String, List<Throwable>> metaMap = ATS.thriftCompact()
        .map(anoaHandler::<byte[]>of)
        .map(AvroDecoders.binary(anoaHandler, ATS.avroClass))
        .peek(a -> Assert.assertFalse(a.isPresent()))
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(Throwable::toString));

    Assert.assertEquals(1, metaMap.size());
    List<Throwable> throwables = metaMap.values().stream().findFirst().get();
    throwables.stream().forEach(t -> Assert.assertTrue(t instanceof AvroRuntimeException));
    Assert.assertEquals(ATS.n, (long) metaMap.values().stream().findFirst().get().size());
  }
}
