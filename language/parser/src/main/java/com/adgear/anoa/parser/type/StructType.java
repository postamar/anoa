package com.adgear.anoa.parser.type;

import com.adgear.anoa.parser.SchemaGenerator;
import com.adgear.anoa.parser.state.StructState;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.Optional;

final class StructType implements FieldType {

  final StructState enumState;

  public StructType(StructState enumState) {
    this.enumState = enumState;
  }

  @Override
  public Optional<SchemaGenerator> getDependency() {
    return Optional.of(enumState);
  }

  @Override
  public String protoType() {
    return enumState.protoType();
  }

  @Override
  public Optional<String> protoOptions() {
    return Optional.empty();
  }

  @Override
  public String thriftType() {
    return enumState.thriftType();
  }

  @Override
  public Optional<String> thriftDefault() {
    return Optional.empty();
  }

  @Override
  public Schema avroSchema() {
    return enumState.avroSchema();
  }

  @Override
  public Optional<JsonNode> avroDefault() {
    return Optional.empty();
  }

}
