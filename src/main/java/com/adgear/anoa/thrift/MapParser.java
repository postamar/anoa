package com.adgear.anoa.thrift;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.thrift.TException;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.protocol.TType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class MapParser extends JsonNodeParser<Map<String,Object>> {

  final JsonNodeParser valueElementParser;

  MapParser(MapMetaData metaData) {
    if (metaData.keyMetaData.type != TType.STRING) {
      throw new RuntimeException("Map key type is not string.");
    }
    this.valueElementParser = create(metaData.valueMetaData);
  }

  @Override
  Map<String, Object> parse(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isObject()) {
      return null;
    }
    Map<String,Object> result = new HashMap<>();
    Iterator<Map.Entry<String,JsonNode>> iterator = node.fields();
    while (iterator.hasNext()) {
      Map.Entry<String,JsonNode> entry = iterator.next();
      result.put(entry.getKey(), valueElementParser.parse(entry.getValue()));
    }
    return result;
  }

  @Override
  Map<String, Object> parseStrict(JsonNode node) throws TException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isObject()) {
      throw new TException(node.asText() + " is not an object node.");
    }
    Map<String,Object> result = new HashMap<>();
    Iterator<Map.Entry<String,JsonNode>> iterator = node.fields();
    while (iterator.hasNext()) {
      Map.Entry<String,JsonNode> entry = iterator.next();
      result.put(entry.getKey(), valueElementParser.parseStrict(entry.getValue()));
    }
    return result;
  }
}
