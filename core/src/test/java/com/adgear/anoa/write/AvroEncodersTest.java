package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.AvroDecoders;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroEncodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(AvroEncoders.binary(BidReqs.avroSchema))
            .map(AvroDecoders.binary(BidReqs.avroSchema)));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(AvroEncoders.binary(BidReqs.avroClass))
            .map(AvroDecoders.binary(BidReqs.avroSchema)));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(AvroEncoders.json(BidReqs.avroSchema))
            .map(AvroDecoders.json(BidReqs.avroSchema)));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(AvroEncoders.json(BidReqs.avroClass))
            .map(AvroDecoders.json(BidReqs.avroSchema)));
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

  @Test
  public void testAnoaBinary() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(anoaHandler::<GenericRecord>of)
            .map(AvroEncoders.binary(anoaHandler, BidReqs.avroSchema))
            .map(AvroDecoders.binary(anoaHandler, BidReqs.avroSchema))
            .flatMap(Anoa::asStream));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(anoaHandler::<open_rtb.BidRequestAvro>of)
            .map(AvroEncoders.binary(anoaHandler, BidReqs.avroClass))
            .map(AvroDecoders.binary(anoaHandler, BidReqs.avroSchema))
            .flatMap(Anoa::asStream));
  }


  @Test
  public void testAnoaJson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(anoaHandler::<GenericRecord>of)
            .map(AvroEncoders.json(anoaHandler, BidReqs.avroSchema))
            .map(AvroDecoders.json(anoaHandler, BidReqs.avroSchema))
            .flatMap(Anoa::asStream));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(anoaHandler::<open_rtb.BidRequestAvro>of)
            .map(AvroEncoders.json(anoaHandler, BidReqs.avroClass))
            .map(AvroDecoders.json(anoaHandler, BidReqs.avroSchema))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroGeneric()
            .map(anoaHandler::<GenericRecord>of)
            .map(AvroEncoders.jackson(anoaHandler,
                                      BidReqs.avroSchema,
                                      () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .flatMap(Anoa::asStream)
            .map(AvroDecoders.jackson(BidReqs.avroSchema, true)));

    BidReqs.assertAvroGenerics(
        BidReqs.avroSpecific()
            .map(anoaHandler::<open_rtb.BidRequestAvro>of)
            .map(AvroEncoders.jackson(anoaHandler,
                                      BidReqs.avroClass,
                                      () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .flatMap(Anoa::asStream)
            .map(AvroDecoders.jackson(BidReqs.avroSchema, true)));
  }
}
