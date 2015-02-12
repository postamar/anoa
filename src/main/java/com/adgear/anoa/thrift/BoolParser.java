package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;


class BoolParser extends JsonNodeParser<Boolean> {

  @Override
  Boolean parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isBoolean()) {
      return null;
    }
    return node.asBoolean();
  }

  @Override
  Boolean parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isBoolean()) {
      throw new TException(node.asText() + " is not a boolean.");
    }
    return node.asBoolean();
  }
}
