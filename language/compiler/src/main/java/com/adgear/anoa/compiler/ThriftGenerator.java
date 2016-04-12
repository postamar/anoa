package com.adgear.anoa.compiler;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * .thrift schema generator and TEnum / TBase java source code generator.
 */
final class ThriftGenerator extends GeneratorBase {

  final String thriftCommand;

  ThriftGenerator(CompilationUnit pg, Consumer<String> logger, String thriftCommand) {
    super(pg, "Thrift", logger);
    this.thriftCommand = thriftCommand;
  }

  @Override
  protected String schemaFileName(String namespace) {
    return namespace.replace('.', '_') + ".thrift";
  }

  @Override
  public String generateSchema() {
    StringBuilder sb = new StringBuilder()
        .append("namespace * ").append(protocol.getNamespace()).append("\n");
    getImports()
        .map(getSchemaFile().getParentFile().toPath()::relativize)
        .forEach(path -> sb.append("include \"").append(path).append("\"\n"));
    protocol.getTypes().forEach(s -> sb.append('\n').append(getSchema(s)));
    return sb.toString();
  }

  private String getSchema(Schema type) {
    StringBuilder sb = new StringBuilder();
    sb.append(comments(type.getDoc(), "", "\n"));
    int ordinal = 0;
    if (type.getType() == Schema.Type.ENUM) {
      sb.append("enum ").append(type.getName()).append(" {");
      for (String symbol : type.getEnumSymbols()) {
        sb.append("\n  ").append(symbol).append(" = ").append(ordinal++).append(',');
      }
    } else {
      sb.append("struct ").append(type.getName()).append(" {\n");
      for (Schema.Field field : type.getFields()) {
        ++ordinal;
        if (!BooleanNode.TRUE.equals(field.getJsonProp("removed"))) {
          sb.append(comments(field.doc(), "\n  ", ""));
          sb.append("\n  ").append(field.getJsonProp(AnoaParserBase.ORDINAL_PROP_KEY).asLong())
              .append(": optional ").append(fieldType(field.schema()))
              .append(' ').append(field.name());
          fieldDefault(field.schema(), field.defaultValue())
              .ifPresent(v -> sb.append(" = ").append(v));
          sb.append(";\n");
        }
      }
    }
    return sb.append("}\n").toString();
  }

  private String fieldType(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return Optional.ofNullable(schema.getJsonProp(AnoaParserBase.SET_PROP_KEY))
            .filter(JsonNode::asBoolean)
            .map(__ -> "set<").orElse("list<") + wrappedType(schema.getElementType()) + ">";
      case MAP:
        return "map<string," + wrappedType(schema.getValueType()) + ">";
      default:
        return wrappedType(schema);
    }
  }

  static int getThriftPrecision(Schema schema) {
    final long lb = Optional.ofNullable(schema.getJsonProp(AnoaParserBase.LOWER_BOUND_PROP_KEY))
        .map(JsonNode::asLong)
        .orElse(Long.MIN_VALUE);
    final long ub = Optional.ofNullable(schema.getJsonProp(AnoaParserBase.UPPER_BOUND_PROP_KEY))
        .map(JsonNode::asLong)
        .orElse(Long.MAX_VALUE);
    final long b = Math.max(Math.abs(lb), Math.abs(ub));
    return (b < 0x80000000L) ? ((b < 0x8000L) ? ((b < 0x80L) ? 8 : 16) : 32) : 64;
  }

  private String wrappedType(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return "bool";
      case BYTES:
        return "binary";
      case DOUBLE:
      case FLOAT:
        return "double";
      case LONG:
        return "i64";
      case INT:
        switch (getThriftPrecision(schema)) {
          case 8:
            return "byte";
          case 16:
            return "i16";
          default:
            return "i32";
        }
      case STRING:
        return "string";
      default:
        String qualifier = "";
        if (!schema.getNamespace().equals(protocol.getNamespace())) {
          qualifier = schema.getNamespace().replace('.', '_') + ".";
        }
        return qualifier + schema.getName();
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
        return Optional.of(AnoaBinaryNode.valueOf(value).toThriftString());
      case STRING:
        return Optional.of(value.toString());
      case ENUM:
        return Optional.of(schema.getName() + "." + value.asText());
      default:
        return Optional.empty();
    }
  }

  @Override
  public void generateJava(File schemaRootDir, File javaRootDir)
      throws JavaCodeGenerationException {
    Path out = schemaRootDir.toPath().relativize(javaRootDir.toPath());
    runCommand(thriftCommand,
               Stream.of("--out", out.toString(), "--gen", "java"),
               schemaRootDir);
    patchNamespace(javaRootDir);
  }

  private void patchNamespace(File javaSourceDir) throws JavaCodeGenerationException {
    File packageDir = new File(javaSourceDir, getSchemaFile().getParent());
    assert packageDir.exists();
    assert packageDir.isDirectory();
    File[] javaFiles = packageDir.listFiles();
    assert null != javaFiles;
    for (File javaFile : javaFiles) {
      Map<Pattern, String> map = buildPatchMap(javaFile);
      if (!map.isEmpty()) {
        patchJavaSource(javaFile, map);
      }
    }
  }

  private Map<Pattern, String> buildPatchMap(File javaSource) {
    String target = javaSource.getName().substring(0, javaSource.getName().lastIndexOf('.'));
    for (Schema type : protocol.getTypes()) {
      if (type.getName().equals(target) && type.getType() == Schema.Type.RECORD) {
        Map<Pattern, String> map = new LinkedHashMap<>();
        type.getFields().stream()
            .filter(f -> f.schema().getType() == Schema.Type.BYTES)
            .forEach(f -> {
              AnoaBinaryNode node = AnoaBinaryNode.valueOf(f.defaultValue());
              map.put(node.buildThriftBrokenJavaPattern(f.name()), node.toThriftJava());
            });
        return map;
      }
    }
    return new HashMap<>();
  }

  private void patchJavaSource(File javaSource, Map<Pattern, String> map)
      throws JavaCodeGenerationException {
    log("Repairing broken thrift compiler output in '" + javaSource + "'...");
    assert javaSource.canRead();
    assert javaSource.canWrite();
    final List<String> sourceLines;
    try (BufferedReader reader = new BufferedReader(new FileReader(javaSource))) {
      sourceLines = reader.lines().collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    for (Map.Entry<Pattern, String> entry : map.entrySet()) {
      for (int i = 0; i < sourceLines.size(); i++) {
        Matcher matcher = entry.getKey().matcher(sourceLines.get(i));
        if (matcher.matches()) {
          sourceLines.set(i, matcher.group(1) + entry.getValue());
        }
      }
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(javaSource))) {
      for (String line : sourceLines) {
        writer.write(line);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new JavaCodeGenerationException(
          "Error patching broken thift compiler output in'" + javaSource + "'.", e);
    }
    log("Successfully repaired broken thrift compiler output in '" + javaSource + "'.");
  }
}
