package com.adgear.anoa.parser.type;

import com.adgear.anoa.parser.SchemaGenerator;
import com.adgear.anoa.parser.Type;

import org.codehaus.jackson.JsonNode;

import java.util.Optional;

public interface FieldType extends Type {

  Optional<SchemaGenerator> getDependency();

  Optional<String> protoOptions();

  Optional<String> thriftDefault();

  Optional<JsonNode> avroDefault();
}
