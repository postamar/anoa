package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.avro.openrtb.BidRequest;
import com.fasterxml.jackson.core.TreeNode;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroDecodersTest {

  final public GenericDatumReader<GenericRecord> genericReader =
      new GenericDatumReader<>(BidReqs.avroSchema);
  final public GenericDatumReader<BidRequest> specificReader =
      new SpecificDatumReader<>(BidReqs.avroClass);

  @Test
  public void testBinary() {
    BidReqs.assertAvroGenerics(BidReqs.avroBinary().map(AvroDecoders.binary(genericReader)));
    BidReqs.assertAvroSpecifics(BidReqs.avroBinary().map(AvroDecoders.binary(specificReader)));
  }

  @Test
  public void testJson() {
    BidReqs.assertAvroGenerics(BidReqs.avroJson().map(AvroDecoders.json(genericReader)));
    BidReqs.assertAvroSpecifics(BidReqs.avroJson().map(AvroDecoders.json(specificReader)));
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
                                   .map(AvroDecoders.binary(anoaFactory, genericReader))
                                   .map(Anoa::get));

    BidReqs.assertAvroSpecifics(BidReqs.avroBinary()
                                    .map(anoaFactory::<byte[]>wrap)
                                    .map(AvroDecoders.binary(anoaFactory, specificReader))
                                    .map(Anoa::get));
  }

  @Test
  public void testAnoaJson() {
    BidReqs.assertAvroGenerics(
        BidReqs.avroJson()
            .map(anoaFactory::<String>wrap)
            .map(AvroDecoders.json(anoaFactory, genericReader))
            .map(Anoa::get));

    BidReqs.assertAvroSpecifics(
        BidReqs.avroJson()
            .map(anoaFactory::<String>wrap)
            .map(AvroDecoders.json(anoaFactory, specificReader))
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
        .map(AvroDecoders.binary(anoaFactory, specificReader))
        .peek(a -> Assert.assertFalse(a.isPresent()))
        .flatMap(Anoa::meta)
        .collect(Collectors.groupingBy(Throwable::toString));

    Assert.assertEquals(1, metaMap.size());
    List<Throwable> throwables = metaMap.values().stream().findFirst().get();
    throwables.stream().forEach(t -> Assert.assertTrue(t instanceof AvroRuntimeException));
    Assert.assertEquals(BidReqs.n, (long) metaMap.values().stream().findFirst().get().size());
  }
}
