package com.adgear.anoa.test;

import com.adgear.anoa.test.ad_exchange.AdExchangeProtobuf;
import com.adgear.anoa.test.ad_exchange.LogEvent;
import com.adgear.anoa.test.ad_exchange.LogEventAvro;
import com.adgear.anoa.test.ad_exchange.LogEventThrift;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestBuilders {

  final TestSample ts = new TestSample();

  @Test
  public void testAvro() throws Exception {
    for (LogEventAvro expected : ts.avroPojos) {
      LogEvent<LogEventAvro> wrapper = LogEvent.avro(expected);
      LogEventAvro actual = wrapper.get();
      if (!expected.equals(actual)) {
        Assert.assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testAll() throws Exception {
    for (LogEventAvro expected : ts.avroPojos) {
      LogEvent<?> w1 = LogEvent.thrift(LogEvent.avro(expected));
      LogEventAvro a1 =  LogEvent.avro(w1).get();
      if (!expected.equals(a1)) {
        Assert.assertEquals(expected, a1);
      }
      LogEvent<?> w2 = LogEvent.protobuf(w1);
      LogEventAvro a2 =  LogEvent.avro(w1).get();
      if (!expected.equals(a2)) {
        Assert.assertEquals(expected, a2);
      }
      LogEvent<?> w3 = LogEvent.thrift(w2);
      LogEventAvro a3 =  LogEvent.avro(w1).get();
      if (!expected.equals(a3)) {
        Assert.assertEquals(expected, a3);
      }
    }
  }
}
