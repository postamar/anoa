package com.adgear.anoa.library.write;

import com.adgear.anoa.read.ProtobufStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.adgear.anoa.write.ProtobufConsumers;
import com.adgear.anoa.write.WriteConsumer;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.junit.Test;

import java.io.IOException;

public class ProtobufConsumersTest {

  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBinary() {
    ATS.assertProtobufObjects(ProtobufStreams.binary(
        ATS.protobufClass,
        true,
        ATS.allAsInputStream(os -> {
          try (WriteConsumer<AdExchangeProtobuf.LogEvent> writeConsumer = ProtobufConsumers
              .binary(os)) {
            ATS.protobuf().forEach(writeConsumer);
          }
        })));
  }

  @Test
  public void testJackson() throws IOException {
    TokenBuffer tb = new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false);
    try (WriteConsumer<AdExchangeProtobuf.LogEvent> wc =
             ProtobufConsumers.jackson(ATS.protobufClass, tb, true)) {
      ATS.protobuf().forEach(wc);
    }
    ATS.assertProtobufObjects(
        ProtobufStreams.jackson(ATS.protobufClass, true, tb.asParser()));
  }
}
