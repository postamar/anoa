package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final public class CompilationUnit {

  final protected AnoaParser anoaParser;
  private Protocol protocol = null;
  private Consumer<String> logger = null;

  public CompilationUnit(String namespace, File baseDir) throws IOException {
    this(namespace, baseDir, null);
  }

  public CompilationUnit(String namespace, ClassLoader resourceLoader) throws IOException {
    this(namespace, null, resourceLoader);
  }

  public CompilationUnit(String namespace, File baseDir, ClassLoader resourceLoader)
      throws IOException {
    anoaParser = new AnoaParser(namespace, baseDir, resourceLoader);
  }

  public CompilationUnit parse(Consumer<String> logger) throws ParseException {
    protocol = anoaParser.CompilationUnit();
    this.logger = logger;
    return this;
  }

  public AnoaCodeGenerator avroGenerator() {
    return new AvroCodeGenerator(this, logger);
  }

  public AnoaCodeGenerator protobufGenerator() {
    return protobufGenerator("protoc");
  }

  public AnoaCodeGenerator protobufGenerator(String protocCommand) {
    return new ProtobufGenerator(this, logger, protocCommand);
  }

  public AnoaCodeGenerator thriftGenerator() {
    return thriftGenerator("thrift");
  }

  public AnoaCodeGenerator thriftGenerator(String thriftCommand) {
    return new ThriftGenerator(this, logger, thriftCommand);
  }

  Stream<String> getImportedNamespaces() {
    return anoaParser.imports.stream();
  }


  Protocol generate(String suffix) {
    if (protocol == null) {
      throw new IllegalStateException("Cannot generate protocol before parsing.");
    }
    Protocol result = new Protocol(protocol.getName() + suffix, protocol.getNamespace());
    result.setTypes(protocol.getTypes().stream()
                        .map(s -> addSuffix(s, suffix))
                        .collect(Collectors.toList()));
    return result;
  }

  static private Schema addSuffix(Schema schema, String suffix) {
    final Schema result;
    switch (schema.getType()) {
      case ENUM:
        result = Schema.createEnum(schema.getName() + suffix,
                                   schema.getDoc(),
                                   schema.getNamespace(),
                                   schema.getEnumSymbols());
        break;
      case RECORD:
        result = Schema.createRecord(schema.getName() + suffix,
                                     schema.getDoc(),
                                     schema.getNamespace(),
                                     false);
        result.setFields(schema.getFields().stream()
                             .map(f -> addSuffix(f, suffix))
                             .collect(Collectors.toList()));
        break;
      case UNION:
        return Schema.createUnion(schema.getTypes().stream()
                                      .map(s -> addSuffix(s, suffix))
                                      .collect(Collectors.toList()));
      case ARRAY:
        return Schema.createArray(addSuffix(schema.getElementType(), suffix));
      case MAP:
        return Schema.createMap(addSuffix(schema.getValueType(), suffix));
      default:
        return schema;
    }
    schema.getAliases().forEach(result::addAlias);
    schema.getJsonProps().forEach(result::addProp);
    return result;
  }

  static private Schema.Field addSuffix(Schema.Field field, String suffix) {
    Schema.Field result = new Schema.Field(field.name(),
                                           addSuffix(field.schema(), suffix),
                                           field.doc(),
                                           field.defaultValue());
    field.aliases().forEach(result::addAlias);
    field.getJsonProps().forEach(result::addProp);
    return result;
  }
}
