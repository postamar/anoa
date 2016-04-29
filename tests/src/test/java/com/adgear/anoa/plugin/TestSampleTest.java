package com.adgear.anoa.plugin;

import com.adgear.anoa.test.AnoaTestSample;
import com.adgear.anoa.test.TestIOException;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestSampleTest {

  final AnoaTestSample ts = new AnoaTestSample();

  @Test
  public void testJson() {
    Assert.assertEquals(ts.nl, ts.json().count());
  }

  @Test
  public void testAvro() {
    Assert.assertEquals(ts.nl, ts.avroJson().count());
    Assert.assertEquals(ts.nl, ts.avroSpecific().count());
    Assert.assertEquals(ts.nl, ts.avroGeneric().count());
    Assert.assertEquals(ts.nl, ts.avroBinary().count());
  }

  @Test
  public void testAvroBatch() throws IOException {
    int n = 0;
    try (DataFileStream<LogEventAvro> stream =
             new DataFileStream<>(ts.avroBatch(),
                                  new SpecificDatumReader<>(ts.avroClass))) {
      while (stream.hasNext()) {
        stream.next();
        ++n;
      }
    }
    Assert.assertEquals(ts.n, n);
  }

  @Test
  public void testThrift() {
    Assert.assertEquals(ts.nl, ts.thriftJson().count());
    Assert.assertEquals(ts.nl, ts.thrift().count());
    Assert.assertEquals(ts.nl, ts.thriftBinary().count());
    Assert.assertEquals(ts.nl, ts.thriftCompact().count());
  }

  @Test
  public void testProto() {
    Assert.assertEquals(ts.nl, ts.protobufJson().count());
    Assert.assertEquals(ts.nl, ts.protobuf().count());
    Assert.assertEquals(ts.nl, ts.protobufBinary().count());
  }


  @Test
  public void testFail() throws IOException {
    try {
      ts.avroBinaryInputStream(500).skip(501);
    } catch (TestIOException e) {
      Assert.assertEquals(500, e.index);
    }
  }
}
