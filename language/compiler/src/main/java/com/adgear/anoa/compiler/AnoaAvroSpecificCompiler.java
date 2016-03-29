package com.adgear.anoa.compiler;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;

import java.util.Optional;

public class AnoaAvroSpecificCompiler extends SpecificCompiler {

  public AnoaAvroSpecificCompiler(Protocol protocol) {
    super(protocol);
    anoaDefaults();
  }

  public AnoaAvroSpecificCompiler(Schema schema) {
    super(schema);
    anoaDefaults();
  }

  private void anoaDefaults() {
    setTemplateDir("/com/adgear/anoa/avro/");
    setStringType(GenericData.StringType.Utf8);
    setFieldVisibility(FieldVisibility.PRIVATE);
    setCreateSetters(false);
    setOutputCharacterEncoding("UTF-8");
  }

  public String getVersion(Schema schema) {
    if (schema.getType() == Schema.Type.ENUM) {
      return Long.toString(schema.getEnumSymbols().size());
    }
    long largest = 0L;
    for (Schema.Field field : schema.getFields()) {
      largest = Math.max(largest, field.getJsonProp(AnoaParserBase.ORDINAL_PROP_KEY).asLong());
    }
    return Long.toString(largest);
  }

  public Schema.Field getAliasField(Schema.Field field, String alias) {
    return new Schema.Field(
        alias,
        field.schema(),
        field.doc(),
        field.defaultValue(),
        field.order());
  }

  public boolean isDeprecated(JsonProperties schema) {
    return Optional.ofNullable(schema.getJsonProp("deprecated"))
        .map(JsonNode::asBoolean)
        .orElse(false);
  }

  public String anoaInterfaceName(Schema schema) {
    String avroName = mangle(schema.getFullName());
    return avroName.substring(0, avroName.length() - 4);
  }

  public String anoaInterfaceFullName(Schema schema) {
    String avroName = mangle(schema.getFullName());
    return avroName.substring(0, avroName.length() - 4);
  }


  public String viewType(Schema schema) {
    return javaType(schema, true, false);
  }

  public String avroType(Schema schema) {
    return javaType(schema, false, false);
  }

  @Override
  public String javaType(Schema schema) {
    return viewType(schema);
  }

  private String javaType(Schema s, boolean view, boolean boxed) {
    switch (s.getType()) {
      case RECORD:
      case ENUM:
        return view ? anoaInterfaceFullName(s) : mangle(s.getFullName());
      case ARRAY:
        return "java.util.List<" + javaType(s.getElementType(), view, true) + ">";
      case MAP:
        return "java.util.Map<" + (view ? "java.lang.String" : "? extends java.lang.CharSequence") +
               "," + javaType(s.getValueType(), view, true) + ">";
      case STRING:  return view ? "java.lang.String" :
                           (boxed ? "? extends java.lang.CharSequence" : "java.lang.CharSequence");
      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return boxed ? "java.lang.Integer" : "int";
      case LONG:    return boxed ? "java.lang.Long" : "long";
      case FLOAT:   return boxed ? "java.lang.Float" : "float";
      case DOUBLE:  return boxed ? "java.lang.Double" : "double";
      case BOOLEAN: return boxed ? "java.lang.Boolean" : "boolean";
      default: throw new RuntimeException("Unsupported type: " + s);
    }
  }

  public String getView(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
        return mangle(field.name()) + ".asReadOnlyBuffer()";
      case STRING:
        return mangle(field.name()) + ".toString()";
      case ARRAY:
        return "java.util.Collections.unmodifiableList(" + mangle(field.name()) + ".stream()" +
               getListFn(field.schema().getElementType()) +
               ".collect(java.util.stream.Collectors.toList()))";
      case MAP:
        return "java.util.Collections.unmodifiableMap(" + mangle(field.name()) +
               ".entrySet().stream().collect(java.util.stream.Collectors.toMap(" +
               "e -> e.getKey().toString(), " + getMapFn(field.schema().getValueType()) + ")))";
      case ENUM:
      case RECORD:
        return mangle(field.name()) + ".get()";
      default:
        throw new IllegalStateException();
    }
  }

  private String getListFn(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(v -> v.asReadOnlyBuffer())";
      case STRING:
        return ".map(v -> v.toString())";
      case ENUM:
      case RECORD:
        return ".map(v -> v.get())";
    }
    return "";
  }


  private String getMapFn(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> e.getValue().asReadOnlyBuffer()";
      case STRING:
        return "e -> e.getValue().toString()";
      case ENUM:
      case RECORD:
        return "e -> e.getValue().get()";
    }
    return "e -> e.getValue()";
  }


}
