package com.adgear.anoa;

import com.adgear.anoa.factory.AvroGenericStreams;
import com.adgear.anoa.tools.data.Format;
import com.adgear.anoa.tools.runnable.DataTool;

import org.apache.avro.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

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
      InputStream inputStream,
      OutputStream outputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new DataTool<>(null, thriftClass, in, out, inputStream, baos).run();
    return baos.toByteArray();
  }

  static public byte[] convert(Format in,
                               Format out,
                               InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new DataTool<>(null, null, in, out, inputStream, baos).run();
    return baos.toByteArray();
  }

  private InputStream bidreqs() {
    return getClass().getResourceAsStream("/bidreqs.json");
  }

  private Schema schema = com.adgear.avro.openrtb.BidRequest.getClassSchema();

  @Test
  public void testJsonToAvro() {
    AvroGenericStreams.batch(new ByteArrayInputStream(convert(schema, Format.JSON, Format.AVRO, bidreqs())))
        .forEach(System.err::println);

  }
}
