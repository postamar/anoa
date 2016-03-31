package com.adgear.anoa.test;

import com.adgear.anoa.test.ad_exchange.LogEvent;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.stream.Collectors;

public class TestJavaSerialization {

  final TestSample ts = new TestSample();

  @Test
  public void testAvro() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent avro : ts.avroPojos) {
      oos.writeObject(avro);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.avroPojos.size(); i++) {
      LogEventAvro actual = (LogEventAvro) ois.readObject();
      LogEventAvro expected = ts.avroPojos.get(i);
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testProtobuf() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent proto : ts.protobufPojos.stream()
        .map(LogEvent.Protobuf::get)
        .collect(Collectors.toList())) {
      oos.writeObject(proto);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.protobufPojos.size(); i++) {
      Assert.assertEquals(LogEvent.Protobuf.get(ts.protobufPojos.get(i)), ois.readObject());
    }
  }

  @Test
  public void testThrift() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent proto : ts.thriftPojos.stream()
        .map(LogEvent.Thrift::get)
        .collect(Collectors.toList())) {
      oos.writeObject(proto);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.thriftPojos.size(); i++) {
      Assert.assertEquals(LogEvent.Thrift.get(ts.thriftPojos.get(i)), ois.readObject());
    }
  }
}
