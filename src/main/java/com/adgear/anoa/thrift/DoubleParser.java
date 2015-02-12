package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;


class DoubleParser extends JsonNodeParser<Double> {

  @Override
  Double parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isNumber()) {
      return null;
    }
    return node.asDouble();
  }

  @Override
  Double parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isNumber()) {
      throw new TException(node.asText() + " is not a number.");
    }
    return node.asDouble();
  }
}
