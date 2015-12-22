package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import thrift.com.adgear.avro.openrtb.BidRequest;


public class ThriftStreamsTest {

  final public AnoaHandler<String> anoaHandler = AnoaHandler.withFn(Object::toString);

  @Test
  public void testThrift() {
    Assert.assertEquals(
        BidReqs.n,
        BidReqs.thrift().peek(Assert::assertNotNull).count());
  }

  @Test
  public void testBinary() {
    BidReqs.assertThriftObjects(
        ThriftStreams.binary(BidReqs.thriftSupplier, BidReqs.thriftBinary(-1)));
  }

  @Test
  public void testCompact() {
    BidReqs.assertThriftObjects(
        ThriftStreams.compact(BidReqs.thriftSupplier, BidReqs.thriftCompact(-1)));
  }

  @Test
  public void testJson() {
    BidReqs.assertThriftObjects(
        ThriftStreams.json(BidReqs.thriftSupplier, BidReqs.thriftJson(-1)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertThriftObjects(
        ThriftStreams.jackson(BidReqs.thriftClass, true, BidReqs.jsonParser(-1)));
  }

  @Test(expected = RuntimeException.class)
  public void testBinaryFail() throws Exception {
    ThriftStreams.binary(BidReqs.thriftSupplier, BidReqs.thriftBinary(3))
        .peek(System.err::println)
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testCompactFail() throws Exception {
    ThriftStreams.compact(BidReqs.thriftSupplier, BidReqs.thriftCompact(321))
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testJsonFail() throws Exception {
    ThriftStreams.json(BidReqs.thriftSupplier, BidReqs.thriftJson(122))
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testJacksonFail() throws Exception {
    ThriftStreams.jackson(BidReqs.thriftClass, true, BidReqs.jsonParser(1234))
        .forEach(Assert::assertNotNull);
  }

  @Test
  public void testAnoaBinary() {
    List<Anoa<BidRequest, String>> anoas =
        ThriftStreams.binary(anoaHandler,
                             BidReqs.thriftSupplier,
                             BidReqs.thriftBinary(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(25, anoas.stream().filter(Anoa::isPresent).count());
  }

  @Test
  public void testAnoaCompact() {
    List<Anoa<BidRequest, String>> anoas =
        ThriftStreams.compact(anoaHandler,
                              BidReqs.thriftSupplier,
                              BidReqs.thriftCompact(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(35, anoas.stream().filter(Anoa::isPresent).count());
  }

  @Test
  public void testAnoaJson() {
    List<Anoa<BidRequest, String>> anoas =
        ThriftStreams.json(anoaHandler,
                           BidReqs.thriftSupplier,
                           BidReqs.thriftJson(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(16, anoas.stream().filter(Anoa::isPresent).count());
  }


  @Test
  public void testAnoaJackson() {
    List<Anoa<BidRequest, String>> anoas =
        ThriftStreams.jackson(anoaHandler,
                              BidReqs.thriftClass,
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

  @Test
  public void testAnoaBroken() {
    List<Anoa<BidRequest, String>> anoas =
        ThriftStreams.compact(anoaHandler,
                              BidReqs.thriftSupplier,
                              BidReqs.thriftBinary(-1))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(0, anoas.stream().filter(Anoa::isPresent).count());
  }
}
