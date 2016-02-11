package com.adgear.anoa.read;

import com.google.openrtb.OpenRtb;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class ProtobufStreamsTest {

  final public AnoaHandler<String> anoaHandler = AnoaHandler.withFn(Object::toString);

  @Test
  public void testProtobuf() {
    Assert.assertEquals(
        BidReqs.n,
        BidReqs.protobuf().peek(Assert::assertNotNull).count());
  }

  @Test
  public void testBinary() {
    BidReqs.assertProtobufObjects(
        ProtobufStreams.binary(BidReqs.protobufClass, true, BidReqs.protobufBinary(-1)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertProtobufObjects(
        ProtobufStreams.jackson(BidReqs.protobufClass, true, BidReqs.jsonParser(-1)));
  }

  @Test(expected = RuntimeException.class)
  public void testBinaryFail() throws Exception {
    ProtobufStreams.binary(BidReqs.protobufClass, true, BidReqs.thriftBinary(3))
        .peek(System.err::println)
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testJacksonFail() throws Exception {
    ProtobufStreams.jackson(BidReqs.protobufClass, true, BidReqs.jsonParser(1234))
        .forEach(Assert::assertNotNull);
  }

  @Test
  public void testAnoaBinary() {
    List<Anoa<OpenRtb.BidRequest, String>> anoas =
        ProtobufStreams.binary(anoaHandler,
                               BidReqs.protobufClass,
                               true,
                               BidReqs.protobufBinary(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(34, anoas.stream().filter(Anoa::isPresent).count());
  }

  @Test
  public void testAnoaJackson() {
    List<Anoa<OpenRtb.BidRequest, String>> anoas =
        ProtobufStreams.jackson(anoaHandler,
                              BidReqs.protobufClass,
                              true,
                              BidReqs.jsonParser(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(20, anoas.stream().filter(Anoa::isPresent).count());
  }
}
