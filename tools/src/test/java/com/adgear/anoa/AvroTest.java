package com.adgear.anoa;

import com.adgear.anoa.read.AnoaRead;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.avro.file.DataFileWriter;
import org.jooq.lambda.Unchecked;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.stream.Stream;

public class AvroTest {

  @Test
  public void test() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonParser jp = new JsonFactory().createParser(getClass().getResourceAsStream("/bidreqs.json"));
    DataFileWriter<BidRequest> dataFileWriter = AnoaAvro.to(baos, BidRequest.class);
    Stream.generate(() -> true).limit(1000)
        .sequential()
        .map(AnoaRecord::of)
        .map(AnoaFunction.pokemonizeChecked(b -> AnoaRead.biFn(BidRequest.class).apply(jp, b),
                                            AvroTest.class))
        .collect(AnoaCollector.toList())
        .streamPresent()
        .forEach(Unchecked.consumer(dataFileWriter::append));
    dataFileWriter.flush();

    Assert.assertEquals(946,
                        AnoaAvro.from(new ByteArrayInputStream(baos.toByteArray()),
                                      BidRequest.getClassSchema())
                            .count());


  }

}
