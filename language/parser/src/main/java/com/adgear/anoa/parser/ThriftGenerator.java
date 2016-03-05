package com.adgear.anoa.parser;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import java.util.Optional;


public class ThriftGenerator extends SchemaGeneratorBase {

  public ThriftGenerator(ProtocolFactory pg) {
    super(pg, "Thrift", ".thrift");
  }

  @Override
  public String getSchema() {
    StringBuilder sb = new StringBuilder();
    sb.append("namespace * ").append(protocol.getNamespace()).append("\n\n");
    getImports()
        .map(getFile().toPath()::relativize)
        .forEach(path -> sb.append("include \"").append(path).append("\"\n"));
    protocol.getTypes().forEach(s -> sb.append('\n').append(getSchema(s)));
    return sb.toString();
  }

  private String getSchema(Schema type) {
    StringBuilder sb = new StringBuilder();
    int ordinal = 0;
    if (type.getType() == Schema.Type.ENUM) {
      sb.append("\nenum ").append(type.getName()).append(" {");
      for (String symbol : type.getEnumSymbols()) {
        sb.append("\n  ").append(symbol).append(" = ").append(ordinal++).append(',');
      }
    } else {
      sb.append("\nstruct ").append(type.getName()).append(" {");
      for (Schema.Field field : type.getFields()) {
        ++ordinal;
        if (!BooleanNode.TRUE.equals(field.getJsonProp("removed"))) {
          sb.append("\n  ").append(ordinal)
              .append(": optional ").append(fieldType(field.schema()))
              .append(' ').append(field.name());
          fieldDefault(field.schema(), field.defaultValue())
              .ifPresent(v -> sb.append(" = ").append(v));
          sb.append(';');
        }
      }
    }
    return sb.append("\n}\n").toString();
  }

  private String fieldType(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return "list<" + wrappedType(schema.getElementType()) + ">";
      case MAP:
        return "map<string," + wrappedType(schema.getValueType()) + ">";
      case UNION:
        return schema.getTypes().get(1).getFullName();
      default:
        return wrappedType(schema);
    }
  }

  private String wrappedType(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return "bool";
      case BYTES:
        return "binary";
      case DOUBLE:
        return "double";
      case FLOAT:
        return "float";
      case INT:
        return "i32";
      case LONG:
        return "i64";
      case STRING:
        return "string";
      default:
        return schema.getName();
    }
  }

  private Optional<String> fieldDefault(Schema schema, JsonNode value) {
    switch (schema.getType()) {
      case BOOLEAN:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
        return Optional.of(value.asText());
      case BYTES:
      case STRING:
        return Optional.of(value.toString());
      case ENUM:
        return Optional.of(schema.getName() + "." + value.asText());
      default:
        return Optional.empty();
    }
  }
}
