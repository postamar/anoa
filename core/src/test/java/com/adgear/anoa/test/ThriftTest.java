package com.adgear.anoa.test;

import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.read.ThriftDecoders;
import com.adgear.anoa.read.ThriftStreams;
import com.adgear.anoa.write.ThriftEncoders;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import thrift.com.adgear.avro.openrtb.BidRequest;

public class ThriftTest {

  @Test
  public void test() throws Exception {
    final List<BidRequest> collected = new ArrayList<>();
    AnoaFactory<Throwable> f = AnoaFactory.passAlong();
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      try (JsonParser jp = new JsonFactory(new ObjectMapper()).createParser(inputStream)) {
        long total = ThriftStreams.jackson(f, jp, BidRequest.class, true)
            .map(ThriftEncoders.binary(f))
            .map(ThriftDecoders.binary(f, BidRequest::new))
            .map(f.consumer(collected::add))
            .count();
        Assert.assertEquals(946, total);
      }
    }
    Assert.assertEquals(946, collected.stream().filter(BidRequest.class::isInstance).count());
    collected.stream().forEach(Assert::assertNotNull);
  }
}
