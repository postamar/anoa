package com.adgear.anoa.library.write;

import com.adgear.anoa.read.ThriftStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventThrift;
import com.adgear.anoa.write.ThriftConsumers;
import com.adgear.anoa.write.WriteConsumer;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

import java.io.IOException;

public class ThriftConsumersTest {

  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBinary() {
    ATS.assertThriftObjects(ThriftStreams.binary(
        ATS.thriftSupplier,
        ATS.allAsInputStream(os -> {
          try (WriteConsumer<LogEventThrift> writeConsumer = ThriftConsumers
              .binary(os)) {
            ATS.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testCompact() {
    ATS.assertThriftObjects(ThriftStreams.compact(
        ATS.thriftSupplier,
        ATS.allAsInputStream(os -> {
          try (WriteConsumer<LogEventThrift> writeConsumer = ThriftConsumers
              .compact(os)) {
            ATS.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJson() {
    ATS.assertThriftObjects(ThriftStreams.json(
        ATS.thriftSupplier,
        ATS.allAsInputStream(os -> {
          try (WriteConsumer<LogEventThrift> writeConsumer = ThriftConsumers.json(os)) {
            ATS.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJackson() throws IOException {
    TokenBuffer tb = new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false);
    try (WriteConsumer<LogEventThrift> wc = ThriftConsumers.jackson(ATS.thriftClass, tb, true)) {
      ATS.thrift().forEach(wc);
    }
    ATS.assertThriftObjects(ThriftStreams.jackson(ATS.thriftClass, true, tb.asParser()));
  }
}
