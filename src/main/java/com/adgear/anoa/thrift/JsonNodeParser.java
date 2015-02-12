package com.adgear.anoa.thrift;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;


abstract public class JsonNodeParser<E> {

  @SuppressWarnings("unchecked")
  static public <T extends TBase<T,?>> JsonNodeParser<T> create(Class<T> thriftClass) {
    return (JsonNodeParser<T>) create(new StructMetaData(TType.STRUCT, thriftClass));
  }

  static public <T extends TBase<T,?>> T parse(ObjectNode node,
                                               JsonNodeParser<T> parser,
                                               boolean strict) throws TException {
    if (strict) {
      return parser.parseStrict(node);
    } else {
      return parser.parse(node);
    }
  }

  abstract E parse(JsonNode node);

  abstract E parseStrict(JsonNode node) throws TException;

  static JsonNodeParser create(FieldValueMetaData metaData) {
    switch (metaData.type) {
      case TType.BOOL:
        return new BoolParser();
      case TType.BYTE:
        return new ByteParser();
      case TType.DOUBLE:
        return new DoubleParser();
      case TType.ENUM:
        return new EnumParser((EnumMetaData) metaData);
      case TType.I16:
        return new I16Parser();
      case TType.I32:
        return new I32Parser();
      case TType.I64:
        return new I64Parser();
      case TType.LIST:
        return new ListParser((ListMetaData) metaData);
      case TType.MAP:
        return new MapParser((MapMetaData) metaData);
      case TType.SET:
        return new SetParser((SetMetaData) metaData);
      case TType.STRUCT:
        return new StructParser((StructMetaData) metaData);
      case TType.STRING:
        return metaData.isBinary() ? new BinaryParser() : new StringParser();
    }
    throw new RuntimeException("Unknown type in metadata " + metaData);
  }

}
