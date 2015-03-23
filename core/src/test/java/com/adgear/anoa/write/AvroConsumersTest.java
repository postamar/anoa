package com.adgear.anoa.write;


import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.AvroGenericStreams;
import com.adgear.anoa.read.AvroSpecificStreams;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;

public class AvroConsumersTest {

  @Test
  public void testBatch() {
    BidReqs.assertAvroGenerics(AvroGenericStreams.batch(BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.batch(BidReqs.avroSchema, os)) {
            BidReqs.avroGeneric().forEach(wc);
          }
        })));

    BidReqs.assertAvroSpecifics(AvroSpecificStreams.batch(BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<BidRequest> wc = AvroConsumers.batch(BidReqs.avroClass, os)) {
            BidReqs.avroSpecific().forEach(wc);
          }
        })));
  }

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(AvroGenericStreams.binary(BidReqs.avroSchema, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.binary(BidReqs.avroSchema, os)) {
            BidReqs.avroGeneric().forEach(wc);
          }
        })));

    BidReqs.assertAvroSpecifics(AvroSpecificStreams.binary(BidReqs.avroClass, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<BidRequest> wc = AvroConsumers.binary(BidReqs.avroClass, os)) {
            BidReqs.avroSpecific().forEach(wc);
          }
        })));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(AvroGenericStreams.json(BidReqs.avroSchema, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.json(BidReqs.avroSchema, os)) {
            BidReqs.avroGeneric().forEach(wc);
          }
        })));

    BidReqs.assertAvroSpecifics(AvroSpecificStreams.json(BidReqs.avroClass, BidReqs.allAsStream(
        os -> {
          try (WriteConsumer<BidRequest> wc = AvroConsumers.json(BidReqs.avroClass, os)) {
            BidReqs.avroSpecific().forEach(wc);
          }
        })));
  }


  @Test
  public void testJackson() throws IOException {
    TokenBuffer b = new TokenBuffer(BidReqs.objectMapper, false);
    try (WriteConsumer<BidRequest> wc = AvroConsumers.jackson(BidReqs.avroClass, b)) {
      BidReqs.avroSpecific().forEach(wc);
    }
    BidReqs.assertAvroSpecifics(AvroSpecificStreams.jackson(BidReqs.avroClass, true, b.asParser()));

    b = new TokenBuffer(BidReqs.objectMapper, false);
    try (WriteConsumer<GenericRecord> wc = AvroConsumers.jackson(BidReqs.avroSchema, b)) {
      BidReqs.avroGeneric().forEach(wc);
    }
    BidReqs.assertAvroGenerics(AvroGenericStreams.jackson(BidReqs.avroSchema, true, b.asParser()));

  }
}
