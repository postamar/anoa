package com.adgear.anoa.codec.thrift;

import com.adgear.anoa.codec.base.CodecBase;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.thrift.JsonNodeParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;

public class JsonNodeToThrift<T extends TBase<T,?>>
    extends CodecBase<JsonNode,T,JsonNodeToThrift.Counter> {

  static public enum Counter {
    THRIFT_DESERIALIZE_EXCEPTION
  }

  final protected JsonNodeParser<T> parser;
  final protected boolean strict;

  public JsonNodeToThrift(Provider<JsonNode> provider, Class<T> thriftClass) {
    this(provider, thriftClass, false);
  }

  public JsonNodeToThrift(Provider<JsonNode> provider, Class<T> thriftClass, boolean strict) {
    super(provider, Counter.class);
    this.strict = strict;
    this.parser = JsonNodeParser.create(thriftClass);
  }

  @Override
  public T transform(JsonNode input) {
    try {
      return JsonNodeParser.parse((ObjectNode) input, parser, strict);
    } catch (TException e) {
      logger.warn(e.getMessage());
      increment(Counter.THRIFT_DESERIALIZE_EXCEPTION);
      return null;
    }
  }
}
