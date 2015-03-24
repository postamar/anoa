package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroEncodersTest {

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(AvroEncoders.binary(BidReqs.avroSchema))
            .map(AvroDecoders.binary(BidReqs.avroSchema, null)));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(AvroEncoders.binary(BidReqs.avroClass))
            .map(AvroDecoders.binary(BidReqs.avroSchema, null)));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(AvroEncoders.json(BidReqs.avroSchema))
            .map(AvroDecoders.json(BidReqs.avroSchema, null)));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(AvroEncoders.json(BidReqs.avroClass))
            .map(AvroDecoders.json(BidReqs.avroSchema, null)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(AvroEncoders.jackson(BidReqs.avroSchema,
                                      () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(TokenBuffer::asParser)
            .map(AvroDecoders.jackson(BidReqs.avroSchema, true)));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(AvroEncoders.jackson(BidReqs.avroClass,
                                      () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(TokenBuffer::asParser)
            .map(AvroDecoders.jackson(BidReqs.avroSchema, true)));
  }

  final public AnoaFactory<Throwable> anoaFactory = AnoaFactory.passAlong();

  @Test
  public void testAnoaBinary() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(anoaFactory::<GenericRecord>wrap)
            .map(AvroEncoders.binary(anoaFactory, BidReqs.avroSchema))
            .map(AvroDecoders.binary(anoaFactory, BidReqs.avroSchema, null))
            .flatMap(Anoa::asStream));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(anoaFactory::<BidRequest>wrap)
            .map(AvroEncoders.binary(anoaFactory, BidReqs.avroClass))
            .map(AvroDecoders.binary(anoaFactory, BidReqs.avroSchema, null))
            .flatMap(Anoa::asStream));
  }


  @Test
  public void testAnoaJson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(anoaFactory::<GenericRecord>wrap)
            .map(AvroEncoders.json(anoaFactory, BidReqs.avroSchema))
            .map(AvroDecoders.json(anoaFactory, BidReqs.avroSchema, null))
            .flatMap(Anoa::asStream));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(anoaFactory::<BidRequest>wrap)
            .map(AvroEncoders.json(anoaFactory, BidReqs.avroClass))
            .map(AvroDecoders.json(anoaFactory, BidReqs.avroSchema, null))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(anoaFactory::<GenericRecord>wrap)
            .map(AvroEncoders.jackson(anoaFactory,
                                      BidReqs.avroSchema,
                                      () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(anoaFactory.function(TokenBuffer::asParser))
            .flatMap(Anoa::asStream)
            .map(AvroDecoders.jackson(BidReqs.avroSchema, true)));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(anoaFactory::<BidRequest>wrap)
            .map(AvroEncoders.jackson(anoaFactory,
                                      BidReqs.avroClass,
                                      () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(anoaFactory.function(TokenBuffer::asParser))
            .flatMap(Anoa::asStream)
            .map(AvroDecoders.jackson(BidReqs.avroSchema, true)));
  }
}