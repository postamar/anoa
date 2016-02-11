package com.adgear.anoa;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BidReqsTest {

  @Test
  public void test() {

    final int n = BidReqs.n;
    Assert.assertEquals(n, BidReqs.jsonStrings().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.jsonBytes().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.jsonObjects().peek(Assert::assertNotNull).count());

    Assert.assertEquals(n, BidReqs.avroGeneric().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.avroSpecific().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.avroBinary().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.avroJson().peek(Assert::assertNotNull).count());

    Assert.assertEquals(n, BidReqs.thrift().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.thriftBinary().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.thriftCompact().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.thriftJson().peek(Assert::assertNotNull).count());

    Assert.assertEquals(n, BidReqs.protobuf().peek(Assert::assertNotNull).count());
    Assert.assertEquals(n, BidReqs.protobufBinary().peek(Assert::assertNotNull).count());
  }

  @Test
  public void testFail() throws IOException {
    try {
      BidReqs.avroBinary(500).skip(501);
    } catch (TestIOException e) {
      Assert.assertEquals(500, e.index);
    }
  }
}
