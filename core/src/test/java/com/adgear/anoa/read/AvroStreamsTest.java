package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.BidReqs;

import org.junit.Test;

public class AvroStreamsTest {

  @Test
  public void testBatch() {
    BidReqs.assertAvroGenerics(AvroStreams.batch(BidReqs.avroBatch(-1)));
  }

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(
        AvroStreams.binary(BidReqs.avroSchema, BidReqs.avroBinary(-1)));
    BidReqs.assertAvroSpecifics(
        AvroStreams.binary(BidReqs.avroClass, BidReqs.avroBinary(-1)));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(
        AvroStreams.json(BidReqs.avroSchema, BidReqs.avroJson(-1)));
    BidReqs.assertAvroSpecifics(
        AvroStreams.json(BidReqs.avroClass, BidReqs.avroJson(-1)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertAvroGenerics(
        AvroStreams.jackson(BidReqs.avroSchema, true, BidReqs.jsonParser(-1)));
    BidReqs.assertAvroSpecifics(
        AvroStreams.jackson(BidReqs.avroClass, true, BidReqs.jsonParser(-1)));
  }

  final public AnoaFactory<Throwable> anoaFactory = AnoaFactory.passAlong();

  @Test
  public void testAnoaBatch() {
    BidReqs.assertAvroGenerics(
        AvroStreams.batch(anoaFactory, BidReqs.avroBatch(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroStreams.batch(anoaFactory, BidReqs.avroClass, BidReqs.avroBatch(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaBinary() {
    BidReqs.assertAvroGenerics(
        AvroStreams.binary(anoaFactory, BidReqs.avroSchema, BidReqs.avroBinary(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroStreams.binary(anoaFactory, BidReqs.avroClass, BidReqs.avroBinary(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJson() {
    BidReqs.assertAvroGenerics(
        AvroStreams.json(anoaFactory, BidReqs.avroSchema, BidReqs.avroJson(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroStreams.json(anoaFactory, BidReqs.avroClass, BidReqs.avroJson(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertAvroGenerics(
        AvroStreams.jackson(anoaFactory, BidReqs.avroSchema, true, BidReqs.jsonParser(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroStreams.jackson(anoaFactory, BidReqs.avroClass, true, BidReqs.jsonParser(-1))
            .flatMap(Anoa::asStream));
  }



}
