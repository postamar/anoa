package com.adgear.anoa.other;

import com.google.openrtb.OpenRtb;

import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.ProtobufDecoders;
import com.adgear.anoa.read.ProtobufStreams;
import com.adgear.anoa.write.ProtobufEncoders;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ProtobufTest {

  @Test
  public void test() throws Exception {
    final List<OpenRtb.BidRequest> collected = new ArrayList<>();
    AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      try (JsonParser jp = new JsonFactory(new ObjectMapper()).createParser(inputStream)) {
        long total = ProtobufStreams.jackson(anoaHandler, OpenRtb.BidRequest.class, true, jp)
            .map(ProtobufEncoders.binary(anoaHandler))
            .map(ProtobufDecoders.binary(anoaHandler, OpenRtb.BidRequest.class, true))
            .map(anoaHandler.consumer(collected::add))
            .count();
        Assert.assertEquals(BidReqs.n + 1, total);
      }
    }
    Assert.assertEquals(BidReqs.n,
                        collected.stream().filter(OpenRtb.BidRequest.class::isInstance).count());
    collected.stream().forEach(Assert::assertNotNull);
  }
}
