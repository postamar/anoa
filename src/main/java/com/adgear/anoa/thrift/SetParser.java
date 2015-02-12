package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;
import org.apache.thrift.meta_data.SetMetaData;

import java.util.HashSet;
import java.util.Iterator;


class SetParser extends JsonNodeParser<HashSet<Object>> {

  final JsonNodeParser elementParser;

  SetParser(SetMetaData metaData) {
    this.elementParser = create(metaData.elemMetaData);
  }

  @Override
  HashSet<Object> parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isArray()) {
      return null;
    }
    HashSet<Object> result = new HashSet<>();
    Iterator<JsonNode> iterator = node.elements();
    while (iterator.hasNext()) {
      result.add(elementParser.parse(iterator.next()));
    }
    return result;
  }

  @Override
  HashSet<Object> parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isArray()) {
      throw new TException(node.asText() + " is not an array node.");
    }
    HashSet<Object> result = new HashSet<>();
    Iterator<JsonNode> iterator = node.elements();
    while (iterator.hasNext()) {
      result.add(elementParser.parseStrict(iterator.next()));
    }
    return result;
  }
}
