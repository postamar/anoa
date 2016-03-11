package com.adgear.anoa.write;


import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.AvroStreams;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;

public class AvroConsumersTest {

  @Test
  public void testBatch() {
    BidReqs.assertAvroGenerics(AvroStreams.batch(BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.batch(BidReqs.avroSchema, os)) {
            BidReqs.avroGeneric().forEach(wc);
          }
        })));
  }

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(AvroStreams.binary(BidReqs.avroSchema, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.binary(BidReqs.avroSchema, os)) {
            BidReqs.avroGeneric().forEach(wc);
          }
        })));

    BidReqs.assertAvroSpecifics(AvroStreams.binary(BidReqs.avroClass, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<open_rtb.BidRequestAvro> wc = AvroConsumers.binary(BidReqs.avroClass, os)) {
            BidReqs.avroSpecific().forEach(wc);
          }
        })));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(AvroStreams.json(BidReqs.avroSchema, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.json(BidReqs.avroSchema, os)) {
            BidReqs.avroGeneric().forEach(wc);
          }
        })));

    BidReqs.assertAvroSpecifics(AvroStreams.json(BidReqs.avroClass, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<open_rtb.BidRequestAvro> wc = AvroConsumers.json(BidReqs.avroClass, os)) {
            BidReqs.avroSpecific().forEach(wc);
          }
        })));
  }


  @Test
  public void testJackson() throws IOException {
    TokenBuffer b = new TokenBuffer(BidReqs.objectMapper, false);
    try (WriteConsumer<open_rtb.BidRequestAvro> wc = AvroConsumers.jackson(BidReqs.avroClass, b)) {
      BidReqs.avroSpecific().forEach(wc);
    }
    BidReqs.assertAvroSpecifics(AvroStreams.jackson(BidReqs.avroClass, true, b.asParser()));

    b = new TokenBuffer(BidReqs.objectMapper, false);
    try (WriteConsumer<GenericRecord> wc = AvroConsumers.jackson(BidReqs.avroSchema, b)) {
      BidReqs.avroGeneric().forEach(wc);
    }
    BidReqs.assertAvroGenerics(AvroStreams.jackson(BidReqs.avroSchema, true, b.asParser()));

  }
}
