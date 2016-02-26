package com.adgear.anoa.parser.type;

import org.apache.avro.Schema;

import java.util.Optional;

final class WrapperList extends WrapperCollectionBase {

  WrapperList(FieldType wrapped) {
    super(wrapped);
  }

  @Override
  public String protoType() {
    return "repeated " + wrapped.protoType();
  }

  @Override
  public Optional<String> protoOptions() {
    return Optional.of("packed=true");
  }

  @Override
  public String thriftType() {
    return "optional list<" + wrapped.thriftType() + ">";
  }

  @Override
  public Schema avroSchema() {
    return Schema.createArray(wrapped.avroSchema());
  }
}
