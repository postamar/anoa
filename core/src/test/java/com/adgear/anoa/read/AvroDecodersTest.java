package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.BidReqs;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.avro.AvroRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroDecodersTest {

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(BidReqs.avroBinary()
                                   .map(AvroDecoders.binary(BidReqs.avroSchema, null)));
    BidReqs.assertAvroSpecifics(BidReqs.avroBinary()
                                    .map(AvroDecoders.binary(BidReqs.avroClass, null)));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(BidReqs.avroJson()
                                   .map(AvroDecoders.json(BidReqs.avroSchema, null)));
    BidReqs.assertAvroSpecifics(BidReqs.avroJson()
                                    .map(AvroDecoders.json(BidReqs.avroClass, BidRequest::new)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertAvroGenerics(BidReqs.jsonObjects()
                                   .map(TreeNode::traverse)
                                   .map(AvroDecoders.jackson(BidReqs.avroSchema, true)));

    BidReqs.assertAvroSpecifics(BidReqs.jsonObjects()
                                    .map(TreeNode::traverse)
                                    .map(AvroDecoders.jackson(BidReqs.avroClass, true)));
  }

  final public AnoaFactory<Throwable> anoaFactory = AnoaFactory.passAlong();

  @Test
  public void testAnoaBinary() {
    BidReqs.assertAvroGenerics(BidReqs.avroBinary()
                                   .map(anoaFactory::<byte[]>wrap)
                                   .map(AvroDecoders.binary(anoaFactory, BidReqs.avroSchema, null))
                                   .map(Anoa::get));

    BidReqs.assertAvroSpecifics(BidReqs.avroBinary()
                                    .map(anoaFactory::<byte[]>wrap)
                                    .map(AvroDecoders.binary(anoaFactory, BidReqs.avroClass, null))
                                    .map(Anoa::get));
  }

  @Test
  public void testAnoaJson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroJson()
            .map(anoaFactory::<String>wrap)
            .map(AvroDecoders.json(anoaFactory, BidReqs.avroSchema, null))
            .map(Anoa::get));

    BidReqs.assertAvroSpecifics(
        BidReqs.avroJson()
            .map(anoaFactory::<String>wrap)
            .map(AvroDecoders.json(anoaFactory, BidReqs.avroClass, null))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertAvroGenerics(
        BidReqs.jsonObjects()
            .map(anoaFactory::<TreeNode>wrap)
            .map(anoaFactory.function(TreeNode::traverse))
            .map(AvroDecoders.jackson(anoaFactory, BidReqs.avroSchema, false))
            .map(Anoa::get));

    BidReqs.assertAvroSpecifics(
        BidReqs.jsonObjects()
            .map(anoaFactory::<TreeNode>wrap)
            .map(anoaFactory.function(TreeNode::traverse))
            .map(AvroDecoders.jackson(anoaFactory, BidReqs.avroClass, false))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaBroken() {
    Map<String, List<Throwable>> metaMap = BidReqs.thriftCompact()
        .map(anoaFactory::<byte[]>wrap)
        .map(AvroDecoders.binary(anoaFactory, BidReqs.avroClass, null))
        .peek(a -> Assert.assertFalse(a.isPresent()))
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(Throwable::toString));

    Assert.assertEquals(1, metaMap.size());
    List<Throwable> throwables = metaMap.values().stream().findFirst().get();
    throwables.stream().forEach(t -> Assert.assertTrue(t instanceof AvroRuntimeException));
    Assert.assertEquals(BidReqs.n, (long) metaMap.values().stream().findFirst().get().size());
  }
}
