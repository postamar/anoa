package com.adgear.anoa.io.json;

import com.google.common.io.LineReader;

import com.adgear.anoa.io.read.json.JsonReader;
import com.adgear.anoa.io.write.json.JsonWriter;
import com.adgear.avro.openrtb.BidRequest;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

public class AvroWriterTest {

  @Test
  public void test() throws Exception {
    InputStream stream = getClass().getResourceAsStream("/bidreqs.json");
    LineReader reader = new LineReader(new InputStreamReader(stream));
    String line;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    JsonReader<BidRequest> jr = JsonReader.create(BidRequest.class);
    JsonWriter<GenericRecord> jw = JsonWriter.create(BidRequest.getClassSchema());

    while ((line = reader.readLine()) != null) {
      final BidRequest bidRequest = jr.readStrict(JsonReader.createParser(line.getBytes()));
      jw.writeToStream(bidRequest, baos);
      System.err.println(new String(baos.toByteArray()));
      baos.reset();
    }
  }
}
