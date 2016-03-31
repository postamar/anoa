package com.adgear.anoa.compiler;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

abstract class JavaGeneratorBase extends SpecificCompiler {

  final protected String protocolFullName;

  public JavaGeneratorBase(Protocol protocol) {
    super(protocol);
    setStringType(GenericData.StringType.Utf8);
    setFieldVisibility(FieldVisibility.PRIVATE);
    setCreateSetters(false);
    setOutputCharacterEncoding("UTF-8");
    this.protocolFullName = Optional.ofNullable(protocol.getNamespace())
                                .map(ns -> ns + ".")
                                .orElse("")
                            + protocol.getName();
    try {
      Field protocolField = SpecificCompiler.class.getDeclaredField("protocol");
      protocolField.setAccessible(true);
      protocolField.set(this, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String anoaInterfaceName(Schema schema) {
    return mangle(schema.getName());
  }

  public String anoaInterfaceFullName(Schema schema) {
    return mangle(schema.getFullName());
  }

  public List<Schema.Field> fields(Schema schema) {
    return schema.getFields().stream()
        .filter(f -> !"true".equals(f.getProp("removed")))
        .collect(Collectors.toList());
  }

  public String version(Schema schema) {
    if (schema.getType() == Schema.Type.ENUM) {
      return Long.toString(schema.getEnumSymbols().size());
    }
    long largest = 0L;
    for (Schema.Field field : schema.getFields()) {
      largest = Math.max(largest, field.getJsonProp(AnoaParserBase.ORDINAL_PROP_KEY).asLong());
    }
    return Long.toString(largest);
  }

  public boolean isDeprecated(JsonProperties schema) {
    return Optional.ofNullable(schema.getJsonProp("deprecated"))
        .map(JsonNode::asBoolean)
        .orElse(false);
  }

  public Schema.Field aliasField(Schema.Field field, String alias) {
    return new Schema.Field(
        alias,
        field.schema(),
        field.doc(),
        field.defaultValue(),
        field.order());
  }

  @Override
  public String javaType(Schema schema) {
    return getType(schema, true, false, false);
  }

  protected String getType(Schema s, boolean view, boolean boxed, boolean entry) {
    switch (s.getType()) {
      case RECORD:
      case ENUM:
        return anoaInterfaceFullName(s) + (view ? "" : "Avro");
      case ARRAY:
        return (entry ? "" : "java.util.List<") +
               getType(s.getElementType(), view, true, entry) +
               (entry ? "" : ">");
      case MAP:
        return "java.util.Map" + (entry ? ".Entry" : "") +
               " <" + (view ? "java.lang.String" : "java.lang.CharSequence") +
               "," + getType(s.getValueType(), view, true, entry) + ">";
      case STRING:  return view ? "java.lang.String" : "java.lang.CharSequence";
      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return boxed ? "java.lang.Integer" : "int";
      case LONG:    return boxed ? "java.lang.Long" : "long";
      case FLOAT:   return boxed ? "java.lang.Float" : "float";
      case DOUBLE:  return boxed ? "java.lang.Double" : "double";
      case BOOLEAN: return boxed ? "java.lang.Boolean" : "boolean";
      default: throw new RuntimeException("Unsupported type: " + s);
    }
  }
}
