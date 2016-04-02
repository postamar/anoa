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

  public String exportValueType(Schema s) {
    switch (s.getType()) {
      case STRING:  return "java.lang.CharSequence";
      case BYTES:   return "java.util.function.Supplier<byte[]>";
      case INT:     return "java.lang.Integer";
      case LONG:    return "java.lang.Long";
      case FLOAT:   return "java.lang.Float";
      case DOUBLE:  return "java.lang.Double";
      case BOOLEAN: return "java.lang.Boolean";
      default: return anoaInterfaceFullName(s) + "<?>";
    }
  }

  public String exportType(Schema s) {
    switch (s.getType()) {
      case INT:     return "int";
      case LONG:    return "long";
      case FLOAT:   return "float";
      case DOUBLE:  return "double";
      case BOOLEAN: return "boolean";
      case ARRAY:   return "java.util.List<"
                           + exportValueType(s.getElementType()) + ">";
      case MAP:     return "java.util.Map<java.lang.CharSequence,"
                           + exportValueType(s.getValueType()) + ">";
      default: return exportValueType(s);
    }
  }

  static final public String IMPORTED = "instance";
}
