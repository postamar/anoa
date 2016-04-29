package com.adgear.anoa.library.other;

import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.write.ProtobufEncoders;
import com.adgear.anoa.read.ProtobufDecoders;
import com.adgear.anoa.read.ProtobufStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ProtobufTest {

  static final AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void test() throws Exception {
    final List<AdExchangeProtobuf.LogEvent> collected = new ArrayList<>();
    AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
    try (InputStream inputStream = ATS.jsonInputStream(-1)) {
      try (JsonParser jp = new JsonFactory(new ObjectMapper()).createParser(inputStream)) {
        long total = ProtobufStreams.jackson(anoaHandler, ATS.protobufClass, true, jp)
            .map(ProtobufEncoders.binary(anoaHandler))
            .map(ProtobufDecoders.binary(anoaHandler, ATS.protobufClass, true))
            .map(anoaHandler.consumer(collected::add))
            .count();
        Assert.assertEquals(ATS.n + 1, total);
      }
    }
    Assert.assertEquals(ATS.n, collected.stream().filter(ATS.protobufClass::isInstance).count());
    collected.stream().forEach(Assert::assertNotNull);
  }
}
