package com.adgear.anoa.parser;

final public class AvroGenerator extends SchemaGeneratorBase {

  public AvroGenerator(ProtocolFactory pg) {
    super(pg, "Avro", ".avpr");
  }

  @Override
  public String getSchema() {
    return protocol.toString(true);
  }
}
