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
    for (LogEvent avro : ts.avroSpecific()
        .map(LogEvent::avro)
        .collect(Collectors.toList())) {
      oos.writeObject(avro);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.avroPojos.size(); i++) {
      Object object = ois.readObject();
      Assert.assertTrue(object instanceof LogEvent.Avro);
      LogEventAvro actual = ((LogEvent.Avro) object).get();
      LogEventAvro expected = ts.avroPojos.get(i);
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testProtobuf() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent proto : ts.protobufPojos.stream()
        .map(LogEvent::protobuf)
        .collect(Collectors.toList())) {
      oos.writeObject(proto);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.protobufPojos.size(); i++) {
      Assert.assertEquals(LogEvent.protobuf(ts.protobufPojos.get(i)), ois.readObject());
    }
  }

  @Test
  public void testThrift() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent thrift : ts.thriftPojos.stream()
        .map(LogEvent::thrift)
        .collect(Collectors.toList())) {
      oos.writeObject(thrift);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.thriftPojos.size(); i++) {
      Assert.assertEquals(LogEvent.thrift(ts.thriftPojos.get(i)), ois.readObject());
    }
  }


  @Test
  public void testNative() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent nativeImpl : ts.thriftPojos.stream()
        .map(LogEvent::thrift)
        .map(LogEvent::nativeImpl)
        .collect(Collectors.toList())) {
      oos.writeObject(nativeImpl);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.thriftPojos.size(); i++) {
      Object expected = LogEvent.nativeImpl(LogEvent.thrift(ts.thriftPojos.get(i)));
      Object actual = ois.readObject();
      if (!expected.equals(actual)) {
        Assert.assertEquals(expected, actual);
      };
    }
  }
}
