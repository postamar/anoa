package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.test.AnoaTestSample;
import com.fasterxml.jackson.core.TreeNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ThriftDecodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBinary() {
    ATS.assertThriftObjects(
        ATS.thriftBinary().map(ThriftDecoders.binary(ATS.thriftSupplier)));
  }

  @Test
  public void testCompact() {
    ATS.assertThriftObjects(
        ATS.thriftCompact().map(ThriftDecoders.compact(ATS.thriftSupplier)));
  }

  @Test
  public void testJson() {
    ATS.assertThriftObjects(
        ATS.thriftJson()
            .map(String::getBytes)
            .map(ThriftDecoders.json(ATS.thriftSupplier)));
  }

  @Test
  public void testJackson() {
    ATS.assertThriftObjects(
        ATS.jsonObjects()
            .map(TreeNode::traverse)
            .map(ThriftDecoders.jackson(ATS.thriftClass, true)));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertThriftObjects(
        ATS.thriftBinary()
            .map(anoaHandler::<byte[]>of)
            .map(ThriftDecoders.binary(anoaHandler, ATS.thriftSupplier))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaCompact() {
    ATS.assertThriftObjects(
        ATS.thriftCompact()
            .map(anoaHandler::<byte[]>of)
            .map(ThriftDecoders.compact(anoaHandler, ATS.thriftSupplier))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJson() {
    ATS.assertThriftObjects(
        ATS.thriftJson()
            .map(String::getBytes)
            .map(anoaHandler::<byte[]>of)
            .map(ThriftDecoders.json(anoaHandler, ATS.thriftSupplier))
            .map(Anoa::get));
  }


  @Test
  public void testAnoaJackson() {
    ATS.assertThriftObjects(
        ATS.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(ThriftDecoders.jackson(anoaHandler, ATS.thriftClass, true))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaBroken() {
    Map<String, List<Throwable>> metaMap = ATS.json()
        .map(String::getBytes)
        .map(anoaHandler::<byte[]>of)
        .map(ThriftDecoders.compact(anoaHandler, ATS.thriftSupplier))
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(t -> t.getClass().getSimpleName()));

    Assert.assertEquals(2, metaMap.size());
    Assert.assertTrue(metaMap.containsKey("TProtocolException"));
    Assert.assertTrue(metaMap.containsKey("TTransportException"));
    Assert.assertEquals(717, metaMap.get("TProtocolException").size());
    Assert.assertEquals(158, metaMap.get("TTransportException").size());
  }

}
