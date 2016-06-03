package com.adgear.anoa.library.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
import com.adgear.anoa.write.AvroEncoders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class AvroEncodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBinary() {
    ATS.assertAvroGenerics(
        ATS.avroGeneric()
            .map(AvroEncoders.binary(ATS.avroSchema))
            .map(AvroDecoders.binary(ATS.avroSchema)));

    ATS.assertAvroGenerics(
        ATS.avroSpecific()
            .map(AvroEncoders.binary(ATS.avroClass))
            .map(AvroDecoders.binary(ATS.avroSchema)));
  }

  @Test
  public void testJson() {
    ATS.assertAvroGenerics(
        ATS.avroGeneric()
            .map(AvroEncoders.json(ATS.avroSchema))
            .map(AvroDecoders.json(ATS.avroSchema)));

    ATS.assertAvroGenerics(
        ATS.avroSpecific()
            .map(AvroEncoders.json(ATS.avroClass))
            .map(AvroDecoders.json(ATS.avroSchema)));
  }

  @Test
  public void testJackson() {
    ATS.assertAvroGenerics(
        ATS.avroGeneric()
            .map(AvroEncoders.jacksonStrict(
                ATS.avroSchema,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(TokenBuffer::asParser)
            .map(AvroDecoders.jacksonStrict(ATS.avroSchema)));

    ATS.assertAvroGenerics(
        ATS.avroSpecific()
            .map(AvroEncoders.jacksonStrict(
                ATS.avroClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(TokenBuffer::asParser)
            .map(AvroDecoders.jacksonStrict(ATS.avroSchema)));
  }

  @Test
  public void testJacksonStrictness() throws IOException {
    LogEventAvro avro = AvroDecoders.jackson(ATS.avroClass)
        .apply(ATS.jsonNullsObjectParser());
    JsonNode node = AvroEncoders.jackson(ATS.avroClass,
                         () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false))
        .apply(avro).asParser().readValueAsTree();
    Assert.assertTrue(node.isObject());
    Assert.assertEquals(0, node.size());

    ATS.assertAvroGenerics(
        ATS.avroGeneric()
            .map(AvroEncoders.jackson(ATS.avroSchema,
                                      () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(TokenBuffer::asParser)
            .map(AvroDecoders.jackson(ATS.avroSchema)));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertAvroGenerics(
        ATS.avroGeneric()
            .map(anoaHandler::<GenericRecord>of)
            .map(AvroEncoders.binary(anoaHandler, ATS.avroSchema))
            .map(AvroDecoders.binary(anoaHandler, ATS.avroSchema))
            .flatMap(Anoa::asStream));

    ATS.assertAvroGenerics(
        ATS.avroSpecific()
            .map(anoaHandler::<LogEventAvro>of)
            .map(AvroEncoders.binary(anoaHandler, ATS.avroClass))
            .map(AvroDecoders.binary(anoaHandler, ATS.avroSchema))
            .flatMap(Anoa::asStream));
  }


  @Test
  public void testAnoaJson() {
    ATS.assertAvroGenerics(
        ATS.avroGeneric()
            .map(anoaHandler::<GenericRecord>of)
            .map(AvroEncoders.json(anoaHandler, ATS.avroSchema))
            .map(AvroDecoders.json(anoaHandler, ATS.avroSchema))
            .flatMap(Anoa::asStream));

    ATS.assertAvroGenerics(
        ATS.avroSpecific()
            .map(anoaHandler::<LogEventAvro>of)
            .map(AvroEncoders.json(anoaHandler, ATS.avroClass))
            .map(AvroDecoders.json(anoaHandler, ATS.avroSchema))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    ATS.assertAvroGenerics(
        ATS.avroGeneric()
            .map(anoaHandler::<GenericRecord>of)
            .map(AvroEncoders.jacksonStrict(
                anoaHandler,
                ATS.avroSchema,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .flatMap(Anoa::asStream)
            .map(AvroDecoders.jacksonStrict(ATS.avroSchema)));

    ATS.assertAvroGenerics(
        ATS.avroSpecific()
            .map(anoaHandler::<LogEventAvro>of)
            .map(AvroEncoders.jacksonStrict(
                anoaHandler,
                ATS.avroClass,
                () -> new TokenBuffer(AnoaTestSample.OBJECT_MAPPER, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .flatMap(Anoa::asStream)
            .map(AvroDecoders.jacksonStrict(ATS.avroSchema)));
  }
}
