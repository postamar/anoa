package com.adgear.anoa.test;

import com.adgear.anoa.test.ad_exchange.LogEvent;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;

import org.junit.Assert;
import org.junit.Test;

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
      LogEventAvro actual = LogEventAvro.from(LogEvent.Thrift.from(LogEvent.Protobuf.from(LogEvent.Thrift.from(expected)))).get();
      if (!expected.equals(actual)) {
        Assert.assertEquals(expected, actual);
      }
    }
  }

}
