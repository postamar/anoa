package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.BidReqs;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ThriftDecodersTest {

  @Test
  public void testBinary() {
    BidReqs.assertThriftObjects(
        BidReqs.thriftBinary().map(ThriftDecoders.binary(BidReqs.thriftSupplier)));
  }

  @Test
  public void testCompact() {
    BidReqs.assertThriftObjects(
        BidReqs.thriftCompact().map(ThriftDecoders.compact(BidReqs.thriftSupplier)));
  }

  @Test
  public void testJson() {
    BidReqs.assertThriftObjects(
        BidReqs.thriftJson().map(ThriftDecoders.json(BidReqs.thriftSupplier)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertThriftObjects(
        BidReqs.jsonObjects()
            .map(TreeNode::traverse)
            .map(ThriftDecoders.jackson(BidReqs.thriftClass, true)));
  }

  final public AnoaFactory<Throwable> anoaFactory = AnoaFactory.passAlong();

  @Test
  public void testAnoaBinary() {
    BidReqs.assertThriftObjects(
        BidReqs.thriftBinary()
            .map(anoaFactory::<byte[]>wrap)
            .map(ThriftDecoders.binary(anoaFactory, BidReqs.thriftSupplier))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaCompact() {
    BidReqs.assertThriftObjects(
        BidReqs.thriftCompact()
            .map(anoaFactory::<byte[]>wrap)
            .map(ThriftDecoders.compact(anoaFactory, BidReqs.thriftSupplier))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJson() {
    BidReqs.assertThriftObjects(
        BidReqs.thriftJson()
            .map(anoaFactory::<byte[]>wrap)
            .map(ThriftDecoders.json(anoaFactory, BidReqs.thriftSupplier))
            .map(Anoa::get));
  }


  @Test
  public void testAnoaJackson() {
    BidReqs.assertThriftObjects(
        BidReqs.jsonObjects()
            .map(anoaFactory::<TreeNode>wrap)
            .map(anoaFactory.function(TreeNode::traverse))
            .map(ThriftDecoders.jackson(anoaFactory, BidReqs.thriftClass, true))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaBroken() {
    Map<String, List<Throwable>> metaMap = BidReqs.thriftBinary()
        .map(anoaFactory::<byte[]>wrap)
        .map(ThriftDecoders.compact(anoaFactory, BidReqs.thriftSupplier))
        .peek(a -> Assert.assertFalse(a.isPresent()))
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(Throwable::toString));

    Assert.assertEquals(1, metaMap.size());
    List<Throwable> throwables = metaMap.values().stream().findFirst().get();
    throwables.stream().forEach(t -> Assert.assertTrue(t instanceof TException));
    Assert.assertEquals(BidReqs.n, (long) metaMap.values().stream().findFirst().get().size());
  }
}
