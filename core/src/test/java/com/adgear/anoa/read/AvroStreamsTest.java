package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.AvroGenericStreams;
import com.adgear.anoa.read.AvroSpecificStreams;

import org.junit.Test;

public class AvroStreamsTest {

  @Test
  public void testBatch() {
    BidReqs.assertAvroGenerics(AvroGenericStreams.batch(BidReqs.avroBatch(-1)));
    BidReqs.assertAvroSpecifics(AvroSpecificStreams.batch(BidReqs.avroBatch(-1)));
  }

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(
        AvroGenericStreams.binary(BidReqs.avroSchema, BidReqs.avroBinary(-1)));
    BidReqs.assertAvroSpecifics(
        AvroSpecificStreams.binary(BidReqs.avroClass, BidReqs.avroBinary(-1)));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(
        AvroGenericStreams.json(BidReqs.avroSchema, BidReqs.avroJson(-1)));
    BidReqs.assertAvroSpecifics(
        AvroSpecificStreams.json(BidReqs.avroClass, BidReqs.avroJson(-1)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertAvroGenerics(
        AvroGenericStreams.jackson(BidReqs.avroSchema, true, BidReqs.jsonParser(-1)));
    BidReqs.assertAvroSpecifics(
        AvroSpecificStreams.jackson(BidReqs.avroClass, true, BidReqs.jsonParser(-1)));
  }

  final public AnoaFactory<Throwable> anoaFactory = AnoaFactory.passAlong();

  @Test
  public void testAnoaBatch() {
    BidReqs.assertAvroGenerics(
        AvroGenericStreams.batch(anoaFactory, BidReqs.avroBatch(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroSpecificStreams.batch(anoaFactory, BidReqs.avroClass, BidReqs.avroBatch(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaBinary() {
    BidReqs.assertAvroGenerics(
        AvroGenericStreams.binary(anoaFactory, BidReqs.avroSchema, BidReqs.avroBinary(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroSpecificStreams.binary(anoaFactory, BidReqs.avroClass, BidReqs.avroBinary(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJson() {
    BidReqs.assertAvroGenerics(
        AvroGenericStreams.json(anoaFactory, BidReqs.avroSchema, BidReqs.avroJson(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroSpecificStreams.json(anoaFactory, BidReqs.avroClass, BidReqs.avroJson(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertAvroGenerics(
        AvroGenericStreams.jackson(anoaFactory, BidReqs.avroSchema, true, BidReqs.jsonParser(-1))
            .flatMap(Anoa::asStream));
    BidReqs.assertAvroSpecifics(
        AvroSpecificStreams.jackson(anoaFactory, BidReqs.avroClass, true, BidReqs.jsonParser(-1))
            .flatMap(Anoa::asStream));
  }



}
