package com.adgear.anoa.library.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.test.AnoaTestSample;

import org.junit.Test;

public class AvroStreamsTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();

  @Test
  public void testBatch() {
    ATS.assertAvroGenerics(AvroStreams.batch(ATS.avroBatch()));
  }

  @Test
  public void testBinary() {
    ATS.assertAvroGenerics(
        AvroStreams.binary(ATS.avroSchema, ATS.avroBinaryInputStream(-1)));
    ATS.assertAvroSpecifics(
        AvroStreams.binary(ATS.avroClass, ATS.avroBinaryInputStream(-1)));
  }

  @Test
  public void testJson() {
    ATS.assertAvroGenerics(
        AvroStreams.json(ATS.avroSchema, ATS.avroJsonInputStream(-1)));
    ATS.assertAvroSpecifics(
        AvroStreams.json(ATS.avroClass, ATS.avroJsonInputStream(-1)));
  }

  @Test
  public void testJackson() {
    ATS.assertAvroGenerics(
        AvroStreams.jacksonStrict(ATS.avroSchema, ATS.jsonParser(-1)));
    ATS.assertAvroSpecifics(
        AvroStreams.jacksonStrict(ATS.avroClass, ATS.jsonParser(-1)));
  }

  @Test
  public void testAnoaBatch() {
    ATS.assertAvroGenerics(
        AvroStreams.batch(anoaHandler, ATS.avroBatchInputStream(-1))
            .flatMap(Anoa::asStream));
    ATS.assertAvroSpecifics(
        AvroStreams.batch(anoaHandler, ATS.avroClass, ATS.avroBatchInputStream(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertAvroGenerics(
        AvroStreams.binary(anoaHandler, ATS.avroSchema, ATS.avroBinaryInputStream(-1))
            .flatMap(Anoa::asStream));
    ATS.assertAvroSpecifics(
        AvroStreams.binary(anoaHandler, ATS.avroClass, ATS.avroBinaryInputStream(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJson() {
    ATS.assertAvroGenerics(
        AvroStreams.json(anoaHandler, ATS.avroSchema, ATS.avroJsonInputStream(-1))
            .flatMap(Anoa::asStream));
    ATS.assertAvroSpecifics(
        AvroStreams.json(anoaHandler, ATS.avroClass, ATS.avroJsonInputStream(-1))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    ATS.assertAvroGenerics(
        AvroStreams.jacksonStrict(anoaHandler, ATS.avroSchema, ATS.jsonParser(-1))
            .flatMap(Anoa::asStream));
    ATS.assertAvroSpecifics(
        AvroStreams.jacksonStrict(anoaHandler, ATS.avroClass, ATS.jsonParser(-1))
            .flatMap(Anoa::asStream));
  }


}
