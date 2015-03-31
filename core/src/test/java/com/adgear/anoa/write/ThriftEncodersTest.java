package com.adgear.anoa.write;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.adgear.anoa.BidReqs;
import com.adgear.anoa.read.ThriftDecoders;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.jooq.lambda.Unchecked;
import org.junit.Test;

import thrift.com.adgear.avro.openrtb.BidRequest;

public class ThriftEncodersTest {

  @Test
  public void testBinary() {
    BidReqs.assertThriftObjects(BidReqs.thrift()
                                    .map(ThriftEncoders.binary())
                                    .map(ThriftDecoders.binary(BidRequest::new)));
  }

  @Test
  public void testCompact() {
    BidReqs.assertThriftObjects(BidReqs.thrift()
                                    .map(ThriftEncoders.compact())
                                    .map(ThriftDecoders.compact(BidRequest::new)));
  }

  @Test
  public void testJson() {
    BidReqs.assertThriftObjects(BidReqs.thrift()
                                    .map(ThriftEncoders.json())
                                    .map(ThriftDecoders.json(BidRequest::new)));
  }

  @Test
  public void testJackson() {
    BidReqs.assertJsonObjects(BidReqs.thrift()
                                  .map(ThriftEncoders.jackson(BidReqs.thriftClass, () ->
                                      new TokenBuffer(BidReqs.objectMapper, false)))
                                  .map(TokenBuffer::asParser)
                                  .map(Unchecked.function(JsonParser::readValueAsTree)));
  }

  final public AnoaHandler<Throwable> anoaHandler = AnoaHandler.NO_OP_HANDLER;

  @Test
  public void testAnoaBinary() {
    BidReqs.assertThriftObjects(
        BidReqs.thrift()
            .map(anoaHandler::<BidRequest>of)
            .map(ThriftEncoders.binary(anoaHandler))
            .map(ThriftDecoders.binary(anoaHandler, BidRequest::new))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaCompact() {
    BidReqs.assertThriftObjects(
        BidReqs.thrift()
            .map(anoaHandler::<BidRequest>of)
            .map(ThriftEncoders.compact(anoaHandler))
            .map(ThriftDecoders.compact(anoaHandler, BidRequest::new))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJson() {
    BidReqs.assertThriftObjects(
        BidReqs.thrift()
            .map(anoaHandler::<BidRequest>of)
            .map(ThriftEncoders.json(anoaHandler))
            .map(ThriftDecoders.json(anoaHandler, BidRequest::new))
            .flatMap(Anoa::asStream));
  }

  @Test
  public void testAnoaJackson() {
    BidReqs.assertJsonObjects(
        BidReqs.thrift()
            .map(anoaHandler::<BidRequest>of)
            .map(ThriftEncoders.jackson(anoaHandler,
                                        BidReqs.thriftClass,
                                        () -> new TokenBuffer(BidReqs.objectMapper, false)))
            .map(anoaHandler.function(TokenBuffer::asParser))
            .map(anoaHandler.functionChecked(JsonParser::<ObjectNode>readValueAsTree))
            .flatMap(Anoa::asStream));
  }
}
