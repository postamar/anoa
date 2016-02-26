package com.adgear.anoa.parser.type;

import com.adgear.anoa.parser.SchemaGenerator;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.Optional;

class PrimitiveType implements FieldType {

  PrimitiveType(JsonNode valueNode,
                boolean isProtoDefault,
                String protoType,
                String thriftType,
                Schema.Type avroType) {
    this.valueNode = valueNode;
    this.isProtoDefault = isProtoDefault;
    this.protoType = protoType;
    this.thriftType = thriftType;
    this.avroType = avroType;
  }

  final JsonNode valueNode;
  final boolean isProtoDefault;
  final String protoType;
  final String thriftType;
  final Schema.Type avroType;

  @Override
  public Optional<SchemaGenerator> getDependency() {
    return Optional.empty();
  }

  @Override
  public Schema avroSchema() {
    return Schema.create(avroType);
  }

  @Override
  public String protoType() {
    return protoType;
  }

  @Override
  public String thriftType() {
    return thriftType;
  }

  @Override
  public Optional<String> protoOptions() {
    return isProtoDefault ? Optional.<String>empty() : thriftDefault().map(dv -> "default=" + dv);
  }

  @Override
  public Optional<String> thriftDefault() {
    return Optional.of(valueNode.toString());
  }

  @Override
  public Optional<JsonNode> avroDefault() {
    return Optional.of(valueNode);
  }


}
