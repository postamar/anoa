package com.adgear.anoa.other;

import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.ThriftDecoders;
import com.adgear.anoa.read.ThriftStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventThrift;
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

  static final AnoaTestSample ATS = new AnoaTestSample();

  /*
  @Test
  public void test1() throws Exception {
    try (FileWriter fw = new FileWriter("/tmp/poop")) {
      for (String json : ATS.json().collect(Collectors.toList())) {
        TMemoryBuffer tMemoryBuffer = new TMemoryBuffer(1000);
        TJSONProtocol tjsonProtocol = new TJSONProtocol(tMemoryBuffer);
        ThriftDecoders.jackson(LogEventThrift.class, true).apply(
            new ObjectMapper().readTree(json).traverse()).write(tjsonProtocol);

        fw.write(tMemoryBuffer.toString("UTF-8"));
        fw.write("\n");
      }
      fw.flush();
    }

  }

  */
  @Test
  public void test() throws Exception {
    final List<LogEventThrift> collected = new ArrayList<>();
    AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
    try (InputStream inputStream = ATS.jsonInputStream(-1)) {
      try (JsonParser jp = new JsonFactory(new ObjectMapper()).createParser(inputStream)) {
        long total = ThriftStreams.jackson(anoaHandler, ATS.thriftClass, true, jp)
            .map(ThriftEncoders.binary(anoaHandler))
            .map(ThriftDecoders.binary(anoaHandler, ATS.thriftSupplier))
            .map(anoaHandler.consumer(collected::add))
            .count();
        Assert.assertEquals(ATS.n + 1, total);
      }
    }
    Assert.assertEquals(ATS.n, collected.stream().filter(ATS.thriftClass::isInstance).count());
    collected.stream().forEach(Assert::assertNotNull);
  }

}
