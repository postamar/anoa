package com.adgear.anoa.parser.type;

import org.apache.avro.Schema;

final class WrapperMap extends WrapperCollectionBase {

  WrapperMap(FieldType wrapped) {
    super(wrapped);
  }

  @Override
  public String protoType() {
    return "optional map<string," + wrapped.protoType() + ">";
  }

  @Override
  public String thriftType() {
    return "optional map<string," + wrapped.thriftType() + ">";
  }

  @Override
  public Schema avroSchema() {
    return Schema.createMap(wrapped.avroSchema());
  }
}
