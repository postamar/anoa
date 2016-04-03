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

  public Generator interfaceGenerator(boolean withAvro, boolean withProtobuf, boolean withThrift) {
    return new InterfaceGenerator(this, logger, withAvro, withProtobuf, withThrift);
  }

  public Generator avroGenerator() {
    return new AvroGenerator(this, logger);
  }

  public Generator protobufGenerator() {
    return protobufGenerator("protoc");
  }

  public Generator protobufGenerator(String protocCommand) {
    return new ProtobufGenerator(this, logger, protocCommand);
  }

  public Generator thriftGenerator() {
    return thriftGenerator("thrift");
  }

  public Generator thriftGenerator(String thriftCommand) {
    return new ThriftGenerator(this, logger, thriftCommand);
  }

  Stream<String> getImportedNamespaces() {
    return anoaParser.getImportedNamespaces();
  }

  Protocol generate(String suffix, boolean includeRemoved) {
    if (protocol == null) {
      throw new IllegalStateException("Cannot generate protocol before parsing.");
    }
    Protocol result = new Protocol(protocol.getName() + suffix, protocol.getNamespace());
    result.setTypes(protocol.getTypes().stream()
                        .map(s -> modifySchema(s, suffix, includeRemoved))
                        .collect(Collectors.toList()));
    return result;
  }

  static Schema modifySchema(Schema schema, String suffix, boolean includeRemoved) {
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
                             .filter(f -> includeRemoved || f.getJsonProp("removed") == null
                                          || !(f.getJsonProp("removed").asBoolean()))
                             .map(f -> modifySchema(f, suffix, includeRemoved))
                             .collect(Collectors.toList()));
        break;
      case ARRAY:
        return Schema.createArray(modifySchema(schema.getElementType(), suffix, includeRemoved));
      case MAP:
        return Schema.createMap(modifySchema(schema.getValueType(), suffix, includeRemoved));
      default:
        return schema;
    }
    schema.getAliases().forEach(result::addAlias);
    schema.getJsonProps().forEach(result::addProp);
    return result;
  }

  static private Schema.Field modifySchema(Schema.Field field, String suffix,
                                           boolean includeRemoved) {
    Schema.Field result = new Schema.Field(field.name(),
                                           modifySchema(field.schema(), suffix, includeRemoved),
                                           field.doc(),
                                           field.defaultValue());
    field.aliases().forEach(result::addAlias);
    field.getJsonProps().forEach(result::addProp);
    return result;
  }
}
