package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;


class ByteParser extends JsonNodeParser<Byte> {

  @Override
  Byte parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isInt()) {
      return null;
    }
    int intValue = node.asInt();
    if (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) {
      return null;
    }
    return (byte) intValue;
  }

  @Override
  Byte parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isInt()) {
      throw new TException(node.asText() + " is not an int.");
    }
    int intValue = node.asInt();
    if (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) {
      throw new TException(node.asText() + " is out of bounds for BYTE.");
    }
    return (byte) intValue;
  }
}