package com.adgear.anoa.write;

import com.google.openrtb.OpenRtb;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.ProtobufDecoders;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

public class ProtobufEncodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;

  @Test
  public void testBinary() {
    BidReqs.assertProtobufObjects(BidReqs.protobuf()
                                    .map(ProtobufEncoders.binary())
                                    .map(ProtobufDecoders.binary(BidReqs.protobufClass, true)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertProtobufObjects(BidReqs.protobuf()
                                      .map(ProtobufEncoders.jackson(BidReqs.protobufClass, () ->
                                          new TokenBuffer(BidReqs.objectMapper, false)))
                                      .map(TokenBuffer::asParser)
                                      .map(ProtobufDecoders.jackson(BidReqs.protobufClass, true)));
  }

  @Test
  public void testAnoaBinary() {
    BidReqs.assertProtobufObjects(
        BidReqs.protobuf()
            .map(anoaHandler::<OpenRtb.BidRequest>of)
            .map(ProtobufEncoders.binary(anoaHandler))
            .map(ProtobufDecoders.binary(anoaHandler, BidReqs.protobufClass, true))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertProtobufObjects(
        BidReqs.protobuf()
            .map(anoaHandler::<OpenRtb.BidRequest>of)
            .map(ProtobufEncoders.jackson(anoaHandler,
                                          BidReqs.protobufClass,
                                          () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .map(ProtobufDecoders.jackson(anoaHandler, BidReqs.protobufClass, true))
            .flatMap(Anoa::asStream));
  }
}
