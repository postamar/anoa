package com.adgear.anoa.compiler;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * .proto schema generator and Protobuf java source code generator.
 */
final class ProtobufGenerator extends GeneratorBase {

  final String protocCommand;

  ProtobufGenerator(CompilationUnit cu, Consumer<String> logger, String protocCommand) {
    super(cu, "", logger);
    this.protocCommand = protocCommand;
  }

  @Override
  protected String schemaFileName(String namespace) {
    return namespace.substring(namespace.lastIndexOf('.') + 1) + "_protobuf.proto";
  }

  @Override
  public String generateSchema() {
    StringBuilder sb = new StringBuilder()
        .append("syntax = \"proto2\";\n\n")
        .append("package ").append(protocol.getNamespace()).append(";\n\n");
    getImports().forEach(path -> sb.append("import \"").append(path).append("\";\n"));
    protocol.getTypes().forEach(s -> sb.append('\n').append(getSchema(s)));
    return sb.toString();
  }

  private String getSchema(Schema type) {
    StringBuilder sb = new StringBuilder();
    int ordinal = 0;
    sb.append(comments(type.getDoc(), "", "\n"));
    if (type.getType() == Schema.Type.ENUM) {
      sb.append("enum ").append(type.getName()).append(" {");
      for (String symbol : type.getEnumSymbols()) {
        sb.append("\n  ").append(symbol).append(" = ").append(ordinal++).append(';');
      }
    } else {
      sb.append("message ").append(type.getName()).append(" {\n");
      StringBuilder reservedOrdinals = new StringBuilder();
      StringBuilder reservedNames = new StringBuilder();
      StringBuilder fields = new StringBuilder();
      for (Schema.Field field : type.getFields()) {
        doField(field, reservedOrdinals, reservedNames, fields);
      }
      if (reservedOrdinals.length() > 0) {
        sb.append("\n  ").append(reservedOrdinals).append(';');
        sb.append("\n  ").append(reservedNames).append(';');
      }
      sb.append(fields);
    }
    return sb.append("}\n").toString();
  }

  private void doField(Schema.Field field,
                       StringBuilder reservedOrdinals,
                       StringBuilder reservedNames,
                       StringBuilder fields) {
    long ordinal = field.getJsonProp(AnoaParserBase.ORDINAL_PROP_KEY).asLong();
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
      fields.append(comments(field.doc(), "\n  ", ""));
      fields.append("\n  ").append(fieldType(field.schema()))
          .append(' ').append(field.name())
          .append(" = ").append(ordinal);
      List<String> options = new ArrayList<>();
      if (BooleanNode.TRUE.equals(field.getJsonProp("deprecated"))) {
        options.add("deprecated=true");
      }
      fieldDefault(field.schema(), field.defaultValue())
          .ifPresent(v -> options.add("default=" + v));
      if (field.schema().getType() == Schema.Type.ARRAY) {
        switch (field.schema().getElementType().getType()) {
          case INT:
          case FLOAT:
          case LONG:
          case DOUBLE:
            options.add("packed=true");
        }
      }
      if (!options.isEmpty()) {
        fields.append(options.stream().collect(Collectors.joining(",", " [", "]")));
      }
      fields.append(";\n");
    }
  }

  private String fieldType(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return "repeated " + wrappedType(schema.getElementType());
      case MAP:
        return "map<string," + wrappedType(schema.getValueType()) + ">";
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
        return protocol.getNamespace().equals(schema.getNamespace())
               ? schema.getName()
               : schema.getFullName();
    }
  }

  private Optional<String> fieldDefault(Schema schema, JsonNode value) {
    switch (schema.getType()) {
      case BOOLEAN:
        return value.asBoolean() ? Optional.of("true") : Optional.<String>empty();
      case BYTES:
        return Optional.of(AnoaBinaryNode.valueOf(value))
            .filter(node -> node.getBinaryValue().length > 0)
            .map(AnoaBinaryNode::toOctalString);
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

  @Override
  public void generateJava(File schemaRootDir, File javaRootDir)
      throws JavaCodeGenerationException {
    runCommand(protocCommand,
               Stream.of("--java_out=" + schemaRootDir.toPath().relativize(javaRootDir.toPath())),
               schemaRootDir);
  }
}
