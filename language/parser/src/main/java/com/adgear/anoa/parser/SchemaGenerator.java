package com.adgear.anoa.parser;

import org.apache.avro.Schema;

import java.util.Optional;

public interface SchemaGenerator {

  String anoaFileName();

  Schema avroSchema();

  String avroFileName();

  String protoSchema();

  String protoFileName();

  String thriftSchema();

  String thriftFileName();

  Optional<String> csvSchema();

  String csvFileName();
}
