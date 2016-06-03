package com.adgear.anoa.library.tools;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.library.tools.runnable.DataTool;
import com.adgear.anoa.library.tools.runnable.Format;
import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.read.CborStreams;
import com.adgear.anoa.test.AnoaTestSample;

import org.apache.avro.Schema;
import org.apache.thrift.TBase;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class DataToolTest {

  static final private AnoaTestSample ATS = new AnoaTestSample();
  final private Schema schema = ATS.avroSchema;

  static public byte[] convert(Schema schema,
                               Format in,
                               Format out,
                               InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new DataTool<>(schema, null, null, in, out, inputStream, baos).run();
    return baos.toByteArray();
  }

  static public <T extends TBase> byte[] convert(
      Class<T> thriftClass,
      Format in,
      Format out,
      InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new DataTool<>(null, thriftClass, null, in, out, inputStream, baos).run();
    return baos.toByteArray();
  }

  private InputStream inputStream() {
    return ATS.jsonInputStream(-1);
  }

  @Test
  public void testJsonToAvro() {
    Assert.assertEquals(
        ATS.nl,
        AvroStreams.batch(AnoaHandler.NO_OP_HANDLER, new ByteArrayInputStream(
            convert(schema, Format.JSON, Format.AVRO, inputStream())))
            .filter(Anoa::isPresent)
            .count());
  }

  @Test
  public void testJsonToThriftToCbor() {
    Assert.assertEquals(
        ATS.nl,
        new CborStreams()
            .from(convert(ATS.thriftClass,
                          Format.THRIFT_JSON,
                          Format.CBOR,
                          new ByteArrayInputStream(
                              convert(ATS.thriftClass,
                                      Format.JSON,
                                      Format.THRIFT_JSON,
                                      inputStream()))))
            .count());
  }
}
