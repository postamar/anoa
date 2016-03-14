package com.adgear.anoa.read;


import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.test.AnoaTestSample;
import com.fasterxml.jackson.core.TreeNode;

import org.junit.Test;

public class ProtobufDecodersTest {

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;
  final static AnoaTestSample ATS = new AnoaTestSample();


  @Test
  public void testBinary() {
    ATS.assertProtobufObjects(
        ATS.protobufBinary().map(ProtobufDecoders.binary(ATS.protobufClass, true)));
  }

  @Test
  public void testJackson() {
    ATS.assertProtobufObjects(
        ATS.jsonObjects()
            .peek(System.err::println)
            .map(TreeNode::traverse)
            .map(ProtobufDecoders.jackson(ATS.protobufClass, true)));
  }

  @Test
  public void testAnoaBinary() {
    ATS.assertProtobufObjects(
        ATS.protobufBinary()
            .map(anoaHandler::<byte[]>of)
            .map(ProtobufDecoders.binary(anoaHandler, ATS.protobufClass, true))
            .map(Anoa::get));
  }

  @Test
  public void testAnoaJackson() {
    ATS.assertProtobufObjects(
        ATS.jsonObjects()
            .map(anoaHandler::<TreeNode>of)
            .map(anoaHandler.function(TreeNode::traverse))
            .map(ProtobufDecoders.jackson(anoaHandler, ATS.protobufClass, true))
            .map(Anoa::get));
  }
}
