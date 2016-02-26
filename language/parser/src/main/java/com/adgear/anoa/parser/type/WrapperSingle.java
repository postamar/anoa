package com.adgear.anoa.parser.type;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import java.util.ArrayList;
import java.util.Optional;

final class WrapperSingle extends WrapperBase {

  public WrapperSingle(FieldType wrapped) {
    super(wrapped);
  }

  @Override
  public String protoType() {
    return "optional " + wrapped.protoType();
  }

  @Override
  public Optional<String> protoOptions() {
    return wrapped.protoOptions();
  }

  @Override
  public String thriftType() {
    return "optional " + wrapped.thriftType();
  }

  @Override
  public Optional<String> thriftDefault() {
    return wrapped.thriftDefault();
  }

  @Override
  public Schema avroSchema() {
    if (wrapped instanceof StructType) {
      ArrayList<Schema> types = new ArrayList<>();
      types.add(Schema.create(Schema.Type.NULL));
      types.add(wrapped.avroSchema());
      return Schema.createUnion(types);
    } else {
      return wrapped.avroSchema();
    }
  }

  @Override
  public Optional<JsonNode> avroDefault() {
    if (wrapped instanceof StructType) {
      return Optional.of(NullNode.getInstance());
    } else {
      return wrapped.avroDefault();
    }
  }
}
