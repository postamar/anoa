package com.adgear.anoa.other;

import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.AnoaJacksonTypeException;
import com.adgear.anoa.BidReqs;
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


public class ThriftTest {

  /*
  @Test(expected = AnoaJacksonTypeException.class)
  public void testMissingFields() throws Exception {
    ThriftStreams.jackson(Simple.class,
                          false,
                          new ObjectMapper().getFactory().createParser("{\"baz\":1.9}"))
        .forEach(System.err::println);
  }

  */
  @Test
  public void test() throws Exception {
    final List<open_rtb.BidRequestThrift> collected = new ArrayList<>();
    AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      try (JsonParser jp = new JsonFactory(new ObjectMapper()).createParser(inputStream)) {
        long total = ThriftStreams.jackson(anoaHandler, BidReqs.thriftClass, true, jp)
            .map(ThriftEncoders.binary(anoaHandler))
            .map(ThriftDecoders.binary(anoaHandler, BidReqs.thriftSupplier))
            .map(anoaHandler.consumer(collected::add))
            .count();
        Assert.assertEquals(BidReqs.n + 1, total);
      }
    }
    Assert.assertEquals(BidReqs.n, collected.stream().filter(BidReqs.thriftClass::isInstance).count());
    collected.stream().forEach(Assert::assertNotNull);
  }

}
