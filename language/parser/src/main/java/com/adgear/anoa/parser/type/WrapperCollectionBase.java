package com.adgear.anoa.parser.type;

import org.codehaus.jackson.JsonNode;

import java.util.Optional;

abstract class WrapperCollectionBase extends WrapperBase {

  protected WrapperCollectionBase(FieldType wrapped) {
    super(wrapped);
  }

  @Override
  public Optional<String> protoOptions() {
    return Optional.empty();
  }

  @Override
  public Optional<String> thriftDefault() {
    return Optional.empty();
  }

  @Override
  public Optional<JsonNode> avroDefault() {
    return Optional.empty();
  }
}
