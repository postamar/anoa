package com.adgear.anoa.parser;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final public class ProtocolFactory {

  final protected AnoaParser anoaParser;
  private Protocol protocol = null;

  public ProtocolFactory(String namespace, File baseDir) throws IOException {
    anoaParser = new AnoaParser(namespace, baseDir);
  }

  public ProtocolFactory(String namespace, ClassLoader resourceLoader) throws IOException {
    anoaParser = new AnoaParser(namespace, resourceLoader);
  }

  public ProtocolFactory parse() throws ParseException {
    protocol = anoaParser.CompilationUnit();
    return this;
  }

  public Stream<String> getImportedNamespaces() {
    return anoaParser.imports.stream();
  }

  public File getFile(String namespace, String extension) {
    String name = namespace + namespace.substring(namespace.lastIndexOf('.'));
    return new File(name.replace('.', File.separatorChar) + extension);
  }

  public Protocol generate(String suffix) {
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
