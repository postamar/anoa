package com.adgear.anoa.io.json;

import com.google.common.io.LineReader;

import com.adgear.anoa.io.read.json.JsonReader;

import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;

import thrift.com.adgear.avro.openrtb.BidRequest;

import static org.junit.Assert.*;

public class ThriftReaderTest {

  @Test
  public void test() throws Exception {

    InputStream stream = getClass().getResourceAsStream("/bidreqs.json");
    LineReader reader = new LineReader(new InputStreamReader(stream));
    String line;

    JsonReader<BidRequest> jr = JsonReader.create(BidRequest.class);

    while ((line = reader.readLine()) != null) {
      BidRequest bidRequest = jr.readStrict(JsonReader.createParser(line.getBytes()));
      assertNotNull(bidRequest);
    }
  }

}
