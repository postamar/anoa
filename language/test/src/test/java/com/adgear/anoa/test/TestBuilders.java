package com.adgear.anoa.test;

import com.adgear.anoa.test.ad_exchange.LogEvent;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;

import org.junit.Assert;
import org.junit.Test;

import open_rtb.Impression;

public class TestBuilders {

  final TestSample ts = new TestSample();

  @Test
  public void testAvro() throws Exception {
    for (LogEventAvro expected : ts.avroPojos) {
      LogEvent<LogEventAvro> wrapper = LogEventAvro.from(expected);
      LogEventAvro actual = wrapper.get();
      if (!expected.equals(actual)) {
        Assert.assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testAll() throws Exception {
    for (LogEventAvro expected : ts.avroPojos) {
      LogEvent<?> w1 = LogEvent.Thrift.from(expected);
      LogEventAvro a1 =  LogEventAvro.from(w1).get();
      if (!expected.equals(a1)) {
        Assert.assertEquals(expected, a1);
      }
      LogEvent<?> w2 = LogEvent.Protobuf.from(w1);
      LogEventAvro a2 =  LogEventAvro.from(w1).get();
      if (!expected.equals(a2)) {
        Assert.assertEquals(expected, a2);
      }
      LogEvent<?> w3 = LogEvent.Thrift.from(w2);
      LogEventAvro a3 =  LogEventAvro.from(w1).get();
      if (!expected.equals(a3)) {
        Assert.assertEquals(expected, a3);
      }
    }
  }
}
