package com.adgear.anoa.library.read;


import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.ProtobufDecoders;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.fasterxml.jackson.core.TreeNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import open_rtb.OpenRtbProtobuf;

public class ProtobufDecodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();


  @Test
  public void testBinary() {
    ATS.assertProtobufObjects(
        ATS.protobufBinary().map(ProtobufDecoders.binaryStrict(ATS.protobufClass)));
  }

  @Test
  public void testJackson() {
    ATS.assertProtobufObjects(
        ATS.jsonObjects()
            .map(TreeNode::traverse)
            .map(ProtobufDecoders.jacksonStrict(ATS.protobufClass)));
  }

  @Test
  public void testJacksonStrictness() throws IOException {
    AdExchangeProtobuf.LogEvent strict = ProtobufDecoders.jacksonStrict(ATS.protobufClass)
        .apply(ATS.jsonNullsObjectParser());
    AdExchangeProtobuf.LogEvent loose = ProtobufDecoders.jackson(ATS.protobufClass)
        .apply(ATS.jsonNullsObjectParser());

    Assert.assertTrue(strict.hasRequest());
    Assert.assertTrue(strict.hasResponse());
    Assert.assertNotNull(strict.getProperties());
    Assert.assertTrue(strict.hasTimestamp());
    Assert.assertTrue(strict.hasType());
    Assert.assertTrue(strict.hasUuid());

    Assert.assertEquals(OpenRtbProtobuf.BidRequest.getDefaultInstance(),
                        strict.getRequest());
    Assert.assertEquals(OpenRtbProtobuf.BidResponse.getDefaultInstance(),
                        strict.getResponse());
    Assert.assertTrue(strict.getProperties().isEmpty());
    Assert.assertEquals(0L,
                        strict.getTimestamp());
    Assert.assertEquals(AdExchangeProtobuf.LogEventType.UNKNOWN_LOG_EVENT_TYPE,
                        strict.getType());
    Assert.assertEquals(AdExchangeProtobuf.LogEvent.getDefaultInstance().getUuid(),
                        strict.getUuid());

    Assert.assertFalse(loose.hasRequest());
    Assert.assertFalse(loose.hasResponse());
    Assert.assertNotNull(loose.getProperties());
    Assert.assertTrue(loose.getProperties().isEmpty());
    Assert.assertFalse(loose.hasTimestamp());
    Assert.assertFalse(loose.hasType());
    Assert.assertFalse(loose.hasUuid());
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertProtobufObjects(
        ATS.protobufBinary()
            .map(anoaHandler::<byte[]>of)
            .map(ProtobufDecoders.binaryStrict(anoaHandler, ATS.protobufClass))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJackson() {
    ATS.assertProtobufObjects(
        ATS.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(ProtobufDecoders.jacksonStrict(anoaHandler, ATS.protobufClass))
            .map(Anoa::get));
  }
}
