package com.adgear.anoa.parser.type;

import com.adgear.anoa.parser.SchemaGenerator;
import com.adgear.anoa.parser.state.EnumState;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

import java.util.Optional;

final class EnumType implements FieldType {

  final EnumState enumState;
  final int defaultOrdinal;
  final private String defaultValue;

  EnumType(EnumState enumState, int defaultOrdinal) {
    this.enumState = enumState;
    this.defaultOrdinal = defaultOrdinal;
    this.defaultValue = enumState.avroSchema().getEnumSymbols().get(defaultOrdinal);
  }

  @Override
  public Optional<SchemaGenerator> getDependency() {
    return Optional.of(enumState);
  }

  @Override
  public Optional<String> protoOptions() {
    return (defaultOrdinal == 0) ? Optional.empty() : Optional.of("default=" + defaultValue);
  }

  @Override
  public Optional<String> thriftDefault() {
    String name = enumState.thriftType();
    int idx = name.lastIndexOf('.');
    return Optional.of(name.substring((idx < 0) ? 0 : (idx + 1)) + "." + defaultValue);
  }

  @Override
  public Optional<JsonNode> avroDefault() {
    return Optional.of(TextNode.valueOf(defaultValue));
  }

  @Override
  public String protoType() {
    return enumState.protoType();
  }

  @Override
  public String thriftType() {
    return enumState.thriftType();
  }

  @Override
  public Schema avroSchema() {
    return enumState.avroSchema();
  }
}
