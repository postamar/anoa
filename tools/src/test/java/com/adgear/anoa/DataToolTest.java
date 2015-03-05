package com.adgear.anoa;

import com.adgear.anoa.factory.AvroGenericStreams;
import com.adgear.anoa.factory.CborObjects;
import com.adgear.anoa.tools.runnable.DataTool;
import com.adgear.anoa.tools.runnable.Format;

import org.apache.avro.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import thrift.com.adgear.avro.openrtb.BidRequest;

public class DataToolTest {

  static public byte[] convert(Schema schema,
                               Format in,
                               Format out,
                               InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new DataTool<>(schema, null, in, out, inputStream, baos).run();
    return baos.toByteArray();
  }

  static public <F extends TFieldIdEnum, T extends TBase<T, F>> byte[] convert(
      Class<T> thriftClass,
      Format in,
      Format out,
      InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new DataTool<>(null, thriftClass, in, out, inputStream, baos).run();
    return baos.toByteArray();
  }

  private InputStream bidreqs() {
    return getClass().getResourceAsStream("/bidreqs.json");
  }

  final private Schema schema = com.adgear.avro.openrtb.BidRequest.getClassSchema();

  @Test
  public void testJsonToAvro() {
    Assert.assertEquals(
        946,
        AvroGenericStreams.batch(
            new ByteArrayInputStream(convert(schema, Format.JSON, Format.AVRO, bidreqs())))
            .map(AnoaRecord::of)
            .collect(AnoaCollector.toList())
            .streamPresent()
            .count());
  }

  @Test
  public void testJsonToThriftToCbor() {
    Assert.assertEquals(
        946,
        new CborObjects()
            .from(convert(BidRequest.class,
                          Format.THRIFT_JSON,
                          Format.CBOR,
                          new ByteArrayInputStream(
                              convert(BidRequest.class,
                                      Format.JSON,
                                      Format.THRIFT_JSON,
                                      bidreqs()))))
            .count());
  }
}
