package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.avro.AvroRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroDecodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(BidReqs.avroBinary()
                                   .map(AvroDecoders.binary(BidReqs.avroSchema)));
    BidReqs.assertAvroSpecifics(BidReqs.avroBinary()
                                    .map(AvroDecoders.binary(BidReqs.avroClass)));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(BidReqs.avroJson()
                                   .map(AvroDecoders.json(BidReqs.avroSchema)));
    BidReqs.assertAvroSpecifics(BidReqs.avroJson()
                                    .map(AvroDecoders.json(BidReqs.avroClass, open_rtb.BidRequestAvro::new)));
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

  @Test
  public void testAnoaBinary() {
    BidReqs.assertAvroGenerics(BidReqs.avroBinary()
                                   .map(anoaHandler::<byte[]>of)
                                   .map(AvroDecoders.binary(anoaHandler, BidReqs.avroSchema))
                                   .map(Anoa::get));

    BidReqs.assertAvroSpecifics(BidReqs.avroBinary()
                                    .map(anoaHandler::<byte[]>of)
                                    .map(AvroDecoders.binary(anoaHandler, BidReqs.avroClass))
                                    .map(Anoa::get));
  }

  @Test
  public void testAnoaJson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroJson()
            .map(anoaHandler::<String>of)
            .map(AvroDecoders.json(anoaHandler, BidReqs.avroSchema))
            .map(Anoa::get));

    BidReqs.assertAvroSpecifics(
        BidReqs.avroJson()
            .map(anoaHandler::<String>of)
            .map(AvroDecoders.json(anoaHandler, BidReqs.avroClass))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertAvroGenerics(
        BidReqs.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(AvroDecoders.jackson(anoaHandler, BidReqs.avroSchema, false))
            .map(Anoa::get));

    BidReqs.assertAvroSpecifics(
        BidReqs.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(AvroDecoders.jackson(anoaHandler, BidReqs.avroClass, false))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaBroken() {
    Map<String, List<Throwable>> metaMap = BidReqs.thriftCompact()
        .map(anoaHandler::<byte[]>of)
        .map(AvroDecoders.binary(anoaHandler, BidReqs.avroClass))
        .peek(a -> Assert.assertFalse(a.isPresent()))
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(Throwable::toString));

    Assert.assertEquals(1, metaMap.size());
    List<Throwable> throwables = metaMap.values().stream().findFirst().get();
    throwables.stream().forEach(t -> Assert.assertTrue(t instanceof AvroRuntimeException));
    Assert.assertEquals(BidReqs.n, (long) metaMap.values().stream().findFirst().get().size());
  }
}
