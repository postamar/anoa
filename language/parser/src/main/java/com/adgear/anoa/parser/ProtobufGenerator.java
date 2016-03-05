package com.adgear.anoa.parser;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BinaryNode;
import org.codehaus.jackson.node.BooleanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

final public class ProtobufGenerator extends SchemaGeneratorBase {

  public ProtobufGenerator(ProtocolFactory pg) {
    super(pg, "", "_protobuf.proto");
  }

  @Override
  public String getSchema() {
    StringBuilder sb = new StringBuilder();
    sb.append("package ").append(protocol.getNamespace()).append(";\n\n");
    getImports().forEach(path -> sb.append("import \"").append(path).append("\";\n"));
    protocol.getTypes().forEach(s -> sb.append('\n').append(getSchema(s)));
    return sb.toString();
  }

  private String getSchema(Schema type) {
    StringBuilder sb = new StringBuilder();
    int ordinal = 0;
    if (type.getType() == Schema.Type.ENUM) {
      sb.append("\nenum ").append(type.getName()).append(" {");
      for (String symbol : type.getEnumSymbols()) {
        sb.append("\n  ").append(symbol).append(" = ").append(ordinal++).append(';');
      }
    } else {
      sb.append("\nmessage ").append(type.getName()).append(" {");
      StringBuilder reservedOrdinals = new StringBuilder();
      StringBuilder reservedNames = new StringBuilder();
      StringBuilder fields = new StringBuilder();
      for (Schema.Field field : type.getFields()) {
        doField(field, ++ordinal, reservedOrdinals, reservedNames, fields);
      }
      if (reservedOrdinals.length() > 0) {
        sb.append("\n  ").append(reservedOrdinals).append(';');
        sb.append("\n  ").append(reservedNames).append(';');
      }
      sb.append(fields);
    }
    return sb.append("\n}\n").toString();
  }

  private void doField(Schema.Field field,
                       int ordinal,
                       StringBuilder reservedOrdinals,
                       StringBuilder reservedNames,
                       StringBuilder fields) {
    for (String alias : field.aliases()) {
      reservedNames.append((reservedNames.length() == 0) ? "reserved \"" : ", \"")
          .append(alias).append('"');
    }
    if (BooleanNode.TRUE.equals(field.getJsonProp("removed"))) {
      reservedOrdinals.append((reservedOrdinals.length() == 0) ? "reserved " : ", ")
          .append(ordinal);
      reservedNames.append((reservedNames.length() == 0) ? "reserved \"" : ", \"")
          .append(field.name()).append('"');
    } else {
      fields.append("\n  ").append(fieldType(field.schema()))
          .append(' ').append(field.name())
          .append(" = ").append(ordinal);
      List<String> options = new ArrayList<>();
      if (BooleanNode.TRUE.equals(field.getJsonProp("deprecated"))) {
        options.add("deprecated=true");
      }
      fieldDefault(field.schema(), field.defaultValue()).ifPresent(v -> options.add("default=" + v));
      if (field.schema().getType() == Schema.Type.ARRAY) {
        options.add("packed=true");
      }
      if (!options.isEmpty()) {
        fields.append(options.stream().collect(Collectors.joining(",", " [", "]")));
      }
      fields.append(';');
    }
  }

  private String fieldType(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return "repeated " + wrappedType(schema.getElementType());
      case MAP:
        return "optional map<string," + wrappedType(schema.getValueType());
      case UNION:
        return "optional " + schema.getTypes().get(1).getFullName();
      default:
        return "optional " + wrappedType(schema);
    }
  }

  private String wrappedType(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return "bool";
      case BYTES:
        return "bytes";
      case DOUBLE:
        return "double";
      case FLOAT:
        return "float";
      case INT:
        return "sint32";
      case LONG:
        return "sint64";
      case STRING:
        return "string";
      default:
        return schema.getFullName();
    }
  }

  private Optional<String> fieldDefault(Schema schema, JsonNode value) {
    switch (schema.getType()) {
      case BOOLEAN:
        return value.asBoolean() ? Optional.of("true") : Optional.<String>empty();
      case BYTES:
        return Optional.of(BinaryNode.valueOf(value.getTextValue().getBytes()))
            .filter(bn -> bn.getBinaryValue().length > 0)
            .map(Object::toString);
      case DOUBLE:
      case FLOAT:
        return Optional.of(value.asDouble()).filter(v -> v != 0).map(Object::toString);
      case INT:
      case LONG:
        return Optional.of(value.asLong()).filter(v -> v != 0).map(Object::toString);
      case STRING:
        return Optional.of(value.toString()).filter(s -> s.length() > 2);
      case ENUM:
        return Optional.of(value.asText())
            .filter(e -> !e.equals(schema.getEnumSymbols().get(0)));
      default:
        return Optional.empty();
    }
  }
}
