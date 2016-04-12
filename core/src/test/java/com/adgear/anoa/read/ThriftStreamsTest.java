package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventThrift;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;


public class ThriftStreamsTest {

  final public AnoaHandler<String> anoaHandler = AnoaHandler.withFn(Object::toString);
  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testThrift() {
    Assert.assertEquals(
        ATS.n,
        ATS.thrift().peek(Assert::assertNotNull).count());
  }

  @Test
  public void testBinary() {
    ATS.assertThriftObjects(
        ThriftStreams.binary(ATS.thriftSupplier, ATS.thriftBinaryInputStream(-1)));
  }

  @Test
  public void testCompact() {
    ATS.assertThriftObjects(
        ThriftStreams.compact(ATS.thriftSupplier, ATS.thriftCompactInputStream(-1)));
  }

  @Test
  public void testJson() {
    ATS.assertThriftObjects(
        ThriftStreams.json(ATS.thriftSupplier, ATS.thriftJsonInputStream(-1)));
  }

  @Test
  public void testJackson() {
    ATS.assertThriftObjects(
        ThriftStreams.jackson(ATS.thriftClass, true, ATS.jsonParser(-1)));
  }

  @Test(expected = RuntimeException.class)
  public void testBinaryFail() throws Exception {
    ThriftStreams.binary(ATS.thriftSupplier, ATS.thriftBinaryInputStream(3))
        .peek(System.err::println)
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testCompactFail() throws Exception {
    ThriftStreams.compact(ATS.thriftSupplier, ATS.thriftCompactInputStream(321))
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testJsonFail() throws Exception {
    ThriftStreams.json(ATS.thriftSupplier, ATS.thriftJsonInputStream(122))
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testJacksonFail() throws Exception {
    ThriftStreams.jackson(ATS.thriftClass, true, ATS.jsonParser(1234))
        .forEach(Assert::assertNotNull);
  }

  @Test
  public void testAnoaBinary() {
    List<Anoa<LogEventThrift, String>> anoas =
        ThriftStreams.binary(anoaHandler,
                             ATS.thriftSupplier,
                             ATS.thriftBinaryInputStream(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(14, anoas.stream().filter(Anoa::isPresent).count());
  }

  @Test
  public void testAnoaCompact() {
    List<Anoa<LogEventThrift, String>> anoas =
        ThriftStreams.compact(anoaHandler,
                              ATS.thriftSupplier,
                              ATS.thriftCompactInputStream(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(24, anoas.stream().filter(Anoa::isPresent).count());
  }

  @Test
  public void testAnoaJson() {
    List<Anoa<LogEventThrift, String>> anoas =
        ThriftStreams.json(anoaHandler,
                           ATS.thriftSupplier,
                           ATS.thriftJsonInputStream(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(8, anoas.stream().filter(Anoa::isPresent).count());
  }


  @Test
  public void testAnoaJackson() {
    List<Anoa<LogEventThrift, String>> anoas =
        ThriftStreams.jackson(anoaHandler,
                              ATS.thriftClass,
                              true,
                              ATS.jsonParser(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(15, anoas.stream().filter(Anoa::isPresent).count());
  }

  @Test
  public void testAnoaBroken() {
    List<Anoa<LogEventThrift, String>> anoas =
        ThriftStreams.compact(anoaHandler,
                              ATS.thriftSupplier,
                              ATS.thriftBinaryInputStream(-1))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(3, anoas.stream().filter(Anoa::isPresent).count());
  }
}
