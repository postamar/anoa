package com.adgear.anoa.library.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.ProtobufStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class ProtobufStreamsTest {

  final public AnoaHandler<String> anoaHandler = AnoaHandler.withFn(Object::toString);
  final static AnoaTestSample ATS = new AnoaTestSample();


  @Test
  public void testProtobuf() {
    Assert.assertEquals(
        ATS.n,
        ATS.protobuf().peek(Assert::assertNotNull).count());
  }

  @Test
  public void testBinary() {
    ATS.assertProtobufObjects(
        ProtobufStreams.binary(ATS.protobufClass, true, ATS.protoBinaryInputStream(-1)));
  }

  @Test
  public void testJackson() {
    ATS.assertProtobufObjects(
        ProtobufStreams.jackson(ATS.protobufClass, true, ATS.jsonParser(-1)));
  }

  @Test(expected = RuntimeException.class)
  public void testBinaryFail() throws Exception {
    ProtobufStreams.binary(ATS.protobufClass, true, ATS.thriftBinaryInputStream(3))
        .peek(System.err::println)
        .forEach(Assert::assertNotNull);
  }

  @Test(expected = RuntimeException.class)
  public void testJacksonFail() throws Exception {
    ProtobufStreams.jackson(ATS.protobufClass, true, ATS.jsonParser(1234))
        .forEach(Assert::assertNotNull);
  }

  @Test
  public void testAnoaBinary() {
    List<Anoa<AdExchangeProtobuf.LogEvent, String>> anoas =
        ProtobufStreams.binary(anoaHandler,
                               ATS.protobufClass,
                               true,
                               ATS.protoBinaryInputStream(12345))
            .collect(Collectors.toList());
    anoas.stream()
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(UnaryOperator.identity()))
        .entrySet()
        .stream()
        .forEach(e -> System.err.format("%d\t= %s\n", e.getValue().size(), e.getKey()));
    Assert.assertEquals(29, anoas.stream().filter(Anoa::isPresent).count());
  }

  @Test
  public void testAnoaJackson() {
    List<Anoa<AdExchangeProtobuf.LogEvent, String>> anoas =
        ProtobufStreams.jackson(anoaHandler,
                              ATS.protobufClass,
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
}
