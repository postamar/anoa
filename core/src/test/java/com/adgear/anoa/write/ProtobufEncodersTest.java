package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.ProtobufDecoders;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

public class ProtobufEncodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBinary() {
    ATS.assertProtobufObjects(
        ATS.protobuf()
            .map(ProtobufEncoders.binary())
            .map(ProtobufDecoders.binary(ATS.protobufClass, true)));
  }

  @Test
  public void testJackson() {
    ATS.assertProtobufObjects(
        ATS.protobuf()
            .map(ProtobufEncoders.jackson(ATS.protobufClass, () ->
                new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(TokenBuffer::asParser)
            .map(ProtobufDecoders.jackson(ATS.protobufClass, true)));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertProtobufObjects(
        ATS.protobuf()
            .map(anoaHandler::<AdExchangeProtobuf.LogEvent>of)
            .map(ProtobufEncoders.binary(anoaHandler))
            .map(ProtobufDecoders.binary(anoaHandler, ATS.protobufClass, true))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    ATS.assertProtobufObjects(
        ATS.protobuf()
            .map(anoaHandler::<AdExchangeProtobuf.LogEvent>of)
            .map(ProtobufEncoders.jackson(
                anoaHandler,
                ATS.protobufClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .map(ProtobufDecoders.jackson(anoaHandler, ATS.protobufClass, true))
            .flatMap(Anoa::asStream));
  }
}
