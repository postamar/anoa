package com.adgear.anoa.read;


import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;
import com.fasterxml.jackson.core.TreeNode;

import org.junit.Test;

public class ProtobufDecodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;

  @Test
  public void testBinary() {
    BidReqs.assertProtobufObjects(
        BidReqs.protobufBinary().map(ProtobufDecoders.binary(BidReqs.protobufClass, true)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertProtobufObjects(
        BidReqs.jsonObjects()
            .map(TreeNode::traverse)
            .map(ProtobufDecoders.jackson(BidReqs.protobufClass, true)));
  }

  @Test
  public void testAnoaBinary() {
    BidReqs.assertProtobufObjects(
        BidReqs.protobufBinary()
            .map(anoaHandler::<byte[]>of)
            .map(ProtobufDecoders.binary(anoaHandler, BidReqs.protobufClass, true))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertProtobufObjects(
        BidReqs.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(ProtobufDecoders.jackson(anoaHandler, BidReqs.protobufClass, true))
            .map(Anoa::get));
  }
}
