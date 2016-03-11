package com.adgear.anoa.write;

import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.ThriftStreams;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

import java.io.IOException;

public class ThriftConsumersTest {

  @Test
  public void testBinary() {
    BidReqs.assertThriftObjects(ThriftStreams.binary(
        BidReqs.thriftSupplier,
        BidReqs.allAsStream(os -> {
          try (WriteConsumer<open_rtb.BidRequestThrift> writeConsumer = ThriftConsumers.binary(os)) {
            BidReqs.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testCompact() {
    BidReqs.assertThriftObjects(ThriftStreams.compact(
        BidReqs.thriftSupplier,
        BidReqs.allAsStream(os -> {
          try (WriteConsumer<open_rtb.BidRequestThrift> writeConsumer = ThriftConsumers.compact(os)) {
            BidReqs.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJson() {
    BidReqs.assertThriftObjects(ThriftStreams.json(
        BidReqs.thriftSupplier,
        BidReqs.allAsStream(os -> {
          try (WriteConsumer<open_rtb.BidRequestThrift> writeConsumer = ThriftConsumers.json(os)) {
            BidReqs.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJackson() throws IOException {
    TokenBuffer tb = new TokenBuffer(BidReqs.objectMapper, false);
    try (WriteConsumer<open_rtb.BidRequestThrift> wc = ThriftConsumers.jackson(BidReqs.thriftClass, tb)) {
      BidReqs.thrift().forEach(wc);
    }
    BidReqs.assertThriftObjects(ThriftStreams.jackson(BidReqs.thriftClass, true, tb.asParser()));
  }
}
