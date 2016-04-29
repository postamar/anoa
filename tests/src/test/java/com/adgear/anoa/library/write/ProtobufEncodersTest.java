package com.adgear.anoa.library.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.ProtobufDecoders;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.adgear.anoa.write.AvroEncoders;
import com.adgear.anoa.write.ProtobufEncoders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.jooq.lambda.Unchecked;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import junitx.framework.ListAssert;

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
            .map(ProtobufEncoders.jackson(
                ATS.protobufClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false),
                true))
            .map(TokenBuffer::asParser)
            .map(ProtobufDecoders.jackson(ATS.protobufClass, true)));
  }

  @Test
  public void testJacksonStrictness() throws IOException {
    AdExchangeProtobuf.LogEvent proto = ProtobufDecoders.jackson(ATS.protobufClass, false)
        .apply(ATS.jsonNullsObjectParser());
    JsonNode node = ProtobufEncoders.jackson(
        ATS.protobufClass,
        () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false),
        false)
        .apply(proto).asParser().readValueAsTree();
    Assert.assertTrue(node.isObject());
    Assert.assertEquals(0, node.size());

    List<String> one =
        ATS.avroSpecific()
            .map(AvroEncoders.jackson(
                ATS.avroClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false),
                false))
            .map(Unchecked.function(tb -> (ObjectNode) tb.asParser().readValueAsTree()))
            .map(java.lang.Object::toString)
            .collect(Collectors.toList());
    List<String> two =
        ATS.protobuf()
            .map(ProtobufEncoders.jackson(
                ATS.protobufClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false),
                false))
            .map(Unchecked.function(tb -> (ObjectNode) tb.asParser().readValueAsTree()))
            .map(java.lang.Object::toString)
            .collect(Collectors.toList());

    ListAssert.assertEquals(one, two);
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
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false),
                true))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .map(ProtobufDecoders.jackson(anoaHandler, ATS.protobufClass, true))
            .flatMap(Anoa::asStream));
  }
}
