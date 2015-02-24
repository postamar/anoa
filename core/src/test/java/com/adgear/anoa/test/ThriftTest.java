package com.adgear.anoa.test;

import com.adgear.anoa.AnoaCollector;
import com.adgear.anoa.AnoaConsumer;
import com.adgear.anoa.AnoaFunction;
import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.AnoaSummary;
import com.adgear.anoa.AnoaThrift;
import com.adgear.anoa.read.AnoaRead;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.stream.Stream;

import thrift.com.adgear.avro.openrtb.BidRequest;

public class ThriftTest {

  @Test
  public void test() throws Exception {
    final AnoaSummary<BidRequest> collected;
    try (InputStream inputStream = getClass().getResourceAsStream("/bidreqs.json")) {
      try (JsonParser jp = new JsonFactory().createParser(inputStream)) {
         collected = Stream.generate(() -> jp).limit(1000)
             .map(AnoaRecord::of)
             .sequential()
             .map(AnoaRead.anoaFn(BidRequest.class, true))
             .peek(AnoaConsumer.of(System.out::println))
             .map(AnoaFunction.of(AnoaThrift.<BidRequest>toBinaryFn()))
             .map(AnoaFunction.of(AnoaThrift.fromBinaryFn(BidRequest::new)))
             .collect(AnoaCollector.toList());
      }
    }
    collected.streamCounters().map(Object::toString).sorted().forEach(System.err::println);
    Assert.assertEquals(946, collected.streamPresent().filter(BidRequest.class::isInstance).count());
    collected.streamPresent().forEach(Assert::assertNotNull);
  }
}
