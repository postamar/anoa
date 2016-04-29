package com.adgear.anoa.library.write;

import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.WriteConsumer;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;

public class AvroConsumersTest {

  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBatch() {
    ATS.assertAvroGenerics(AvroStreams.batch(ATS.allAsInputStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.batch(ATS.avroSchema, os)) {
            ATS.avroGeneric().forEach(wc);
          }
        })));
  }

  @Test
  public void testBinary() {
    ATS.assertAvroGenerics(AvroStreams.binary(ATS.avroSchema, ATS.allAsInputStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.binary(ATS.avroSchema, os)) {
            ATS.avroGeneric().forEach(wc);
          }
        })));

    ATS.assertAvroSpecifics(AvroStreams.binary(ATS.avroClass, ATS.allAsInputStream(
        os -> {
          try (WriteConsumer<LogEventAvro> wc = AvroConsumers
              .binary(ATS.avroClass, os)) {
            ATS.avroSpecific().forEach(wc);
          }
        })));
  }

  @Test
  public void testJson() {
    ATS.assertAvroGenerics(AvroStreams.json(ATS.avroSchema, ATS.allAsInputStream(
        os -> {
          try (WriteConsumer<GenericRecord> wc = AvroConsumers.json(ATS.avroSchema, os)) {
            ATS.avroGeneric().forEach(wc);
          }
        })));

    ATS.assertAvroSpecifics(AvroStreams.json(ATS.avroClass, ATS.allAsInputStream(
        os -> {
          try (WriteConsumer<LogEventAvro> wc = AvroConsumers.json(ATS.avroClass, os)) {
            ATS.avroSpecific().forEach(wc);
          }
        })));
  }


  @Test
  public void testJackson() throws IOException {
    TokenBuffer b = new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false);
    try (WriteConsumer<LogEventAvro> wc = AvroConsumers.jackson(ATS.avroClass, b, true)) {
      ATS.avroSpecific().forEach(wc);
    }
    ATS.assertAvroSpecifics(AvroStreams.jackson(ATS.avroClass, true, b.asParser()));

    b = new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false);
    try (WriteConsumer<GenericRecord> wc = AvroConsumers.jackson(ATS.avroSchema, b, true)) {
      ATS.avroGeneric().forEach(wc);
    }
    ATS.assertAvroGenerics(AvroStreams.jackson(ATS.avroSchema, true, b.asParser()));

  }
}
