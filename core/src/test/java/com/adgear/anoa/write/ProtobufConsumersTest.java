package com.adgear.anoa.write;

import com.google.openrtb.OpenRtb;

import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.ProtobufStreams;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

import java.io.IOException;

public class ProtobufConsumersTest {

  @Test
  public void testBinary() {
    BidReqs.assertProtobufObjects(ProtobufStreams.binary(
        BidReqs.protobufClass,
        true,
        BidReqs.allAsStream(os -> {
          try (WriteConsumer<OpenRtb.BidRequest> writeConsumer = ProtobufConsumers.binary(os)) {
            BidReqs.protobuf().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJackson() throws IOException {
    TokenBuffer tb = new TokenBuffer(BidReqs.objectMapper, false);
    try (WriteConsumer<OpenRtb.BidRequest> wc =
             ProtobufConsumers.jackson(BidReqs.protobufClass, tb)) {
      BidReqs.protobuf().forEach(wc);
    }
    BidReqs.assertProtobufObjects(
        ProtobufStreams.jackson(BidReqs.protobufClass, true, tb.asParser()));
  }
}
