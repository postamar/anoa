package com.adgear.anoa.parser;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.Optional;

public interface Type {

  String protoType();

  String thriftType();

  Schema avroSchema();
}
