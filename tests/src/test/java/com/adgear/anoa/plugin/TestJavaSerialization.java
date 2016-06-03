package com.adgear.anoa.plugin;

import com.adgear.anoa.test.AnoaTestSample;
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

  final AnoaTestSample ts = new AnoaTestSample();

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
    for (int i = 0; i < ts.avroSpecific().count(); i++) {
      Object object = ois.readObject();
      Assert.assertTrue(object instanceof LogEvent.Avro);
      LogEventAvro actual = ((LogEvent.Avro) object).get();
      LogEventAvro expected = ts.avroSpecific().skip(i).findFirst().get();
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testProtobuf() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent proto : ts.protobuf()
        .map(LogEvent::protobuf)
        .collect(Collectors.toList())) {
      oos.writeObject(proto);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.protobuf().count(); i++) {
      Assert.assertEquals(LogEvent.protobuf(ts.protobuf().skip(i).findFirst().get()),
                          ois.readObject());
    }
  }

  @Test
  public void testThrift() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent thrift : ts.thrift()
        .map(LogEvent::thrift)
        .collect(Collectors.toList())) {
      oos.writeObject(thrift);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.thrift().count(); i++) {
      Assert.assertEquals(LogEvent.thrift(ts.thrift().skip(i).findFirst().get()), ois.readObject());
    }
  }


  @Test
  public void testNative() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    for (LogEvent nativeImpl : ts.thrift()
        .map(LogEvent::thrift)
        .map(LogEvent::nativeImpl)
        .collect(Collectors.toList())) {
      oos.writeObject(nativeImpl);
    }
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    for (int i = 0; i < ts.thrift().count(); i++) {
      Object expected = LogEvent.nativeImpl(LogEvent.thrift(ts.thrift().skip(i).findFirst().get()));
      Object actual = ois.readObject();
      if (!expected.equals(actual)) {
        Assert.assertEquals(expected, actual);
      };
    }
  }
}
