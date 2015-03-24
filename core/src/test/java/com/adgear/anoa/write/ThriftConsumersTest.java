package com.adgear.anoa.write;

import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.ThriftStreams;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

import java.io.IOException;

import thrift.com.adgear.avro.openrtb.BidRequest;

public class ThriftConsumersTest {

  @Test
  public void testBinary() {
    BidReqs.assertThriftObjects(ThriftStreams.binary(
        BidRequest::new,
        BidReqs.allAsStream(os -> {
          try (WriteConsumer<BidRequest> writeConsumer = ThriftConsumers.binary(os)) {
            BidReqs.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testCompact() {
    BidReqs.assertThriftObjects(ThriftStreams.compact(
        BidRequest::new,
        BidReqs.allAsStream(os -> {
          try (WriteConsumer<BidRequest> writeConsumer = ThriftConsumers.compact(os)) {
            BidReqs.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJson() {
    BidReqs.assertThriftObjects(ThriftStreams.json(
        BidRequest::new,
        BidReqs.allAsStream(os -> {
          try (WriteConsumer<BidRequest> writeConsumer = ThriftConsumers.json(os)) {
            BidReqs.thrift().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJackson() throws IOException {
    TokenBuffer tb = new TokenBuffer(BidReqs.objectMapper, false);
    try (WriteConsumer<BidRequest> wc = ThriftConsumers.jackson(BidReqs.thriftClass, tb)) {
      BidReqs.thrift().forEach(wc);
    }
    BidReqs.assertThriftObjects(ThriftStreams.jackson(BidReqs.thriftClass, true, tb.asParser()));
  }
}
