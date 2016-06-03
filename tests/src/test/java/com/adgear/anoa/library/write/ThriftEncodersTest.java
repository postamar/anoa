package com.adgear.anoa.library.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.ThriftDecoders;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventThrift;
import com.adgear.anoa.write.ThriftEncoders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ThriftEncodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBinary() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(ThriftEncoders.binary())
            .map(ThriftDecoders.binary(ATS.thriftSupplier)));
  }

  @Test
  public void testCompact() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(ThriftEncoders.compact())
            .map(ThriftDecoders.compact(ATS.thriftSupplier)));
  }

  @Test
  public void testJson() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(ThriftEncoders.json())
            .map(ThriftDecoders.json(ATS.thriftSupplier)));
  }

  @Test
  public void testJackson() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(ThriftEncoders.jacksonStrict(
                ATS.thriftClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(TokenBuffer::asParser)
            .map(ThriftDecoders.jacksonStrict(ATS.thriftClass)));
  }


  @Test
  public void testJacksonStrictness() throws IOException {
    LogEventThrift thrift = ThriftDecoders.jackson(ATS.thriftClass)
        .apply(ATS.jsonNullsObjectParser());
    JsonNode node = ThriftEncoders.jackson(
        ATS.thriftClass,
        () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false))
        .apply(thrift).asParser().readValueAsTree();
    Assert.assertTrue(node.isObject());
    Assert.assertEquals(1, node.size());
    Assert.assertTrue(node.has("uuid"));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(anoaHandler::<LogEventThrift>of)
            .map(ThriftEncoders.binary(anoaHandler))
            .map(ThriftDecoders.binary(anoaHandler, ATS.thriftSupplier))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaCompact() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(anoaHandler::<LogEventThrift>of)
            .map(ThriftEncoders.compact(anoaHandler))
            .map(ThriftDecoders.compact(anoaHandler, ATS.thriftSupplier))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJson() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(anoaHandler::<LogEventThrift>of)
            .map(ThriftEncoders.json(anoaHandler))
            .map(ThriftDecoders.json(anoaHandler, ATS.thriftSupplier))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    ATS.assertThriftObjects(
        ATS.thrift()
            .map(anoaHandler::<LogEventThrift>of)
            .map(ThriftEncoders.jacksonStrict(
                anoaHandler,
                ATS.thriftClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .map(ThriftDecoders.jacksonStrict(anoaHandler, ATS.thriftClass))
            .flatMap(Anoa::asStream));
  }
}
