package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;


class I64Parser extends JsonNodeParser<Long> {

  @Override
  Long parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isLong()) {
      return null;
    }
    return node.asLong();
  }

  @Override
  Long parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isLong()) {
      throw new TException(node.asText() + " is not a long.");
    }
    return node.asLong();
  }
}
