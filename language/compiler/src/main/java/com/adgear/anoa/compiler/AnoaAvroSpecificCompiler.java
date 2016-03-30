package com.adgear.anoa.compiler;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;

import java.util.Optional;

public class AnoaAvroSpecificCompiler extends SpecificCompiler {

  final private String protocolFullName;

  public AnoaAvroSpecificCompiler(Protocol protocol) {
    super(protocol);
    protocolFullName = Optional.ofNullable(protocol.getNamespace()).map(ns -> ns + ".").orElse("")
                   + protocol.getName();
    anoaDefaults();
  }

  private void anoaDefaults() {
    setTemplateDir("/com/adgear/anoa/avro/");
    setStringType(GenericData.StringType.Utf8);
    setFieldVisibility(FieldVisibility.PRIVATE);
    setCreateSetters(false);
    setOutputCharacterEncoding("UTF-8");
  }

  public String getProtocolFullName() {
    return protocolFullName;
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
    return javaType(schema, true, false, false);
  }

  public String avroType(Schema schema) {
    return javaType(schema, false, false, false);
  }

  public String entryType(Schema schema) {
    return javaType(schema, false, false, true);
  }

  @Override
  public String javaType(Schema schema) {
    return viewType(schema);
  }

  private String javaType(Schema s, boolean view, boolean boxed, boolean entry) {
    switch (s.getType()) {
      case RECORD:
      case ENUM:
        return view ? anoaInterfaceFullName(s) : mangle(s.getFullName());
      case ARRAY:
        return (entry ? "" : "java.util.List<") +
               javaType(s.getElementType(), view, true, entry) +
               (entry ? "" : ">");
      case MAP:
        return "java.util.Map" + (entry ? ".Entry" : "") +
               " <" + (view ? "java.lang.String" : "java.lang.CharSequence") +
               "," + javaType(s.getValueType(), view, true, entry) + ">";
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

  static final public String ENCODER = "_encoder";

  public String fieldEncoder(Schema.Field field){
    final String name = mangle(field.name());
    switch (field.schema().getType()) {
      case ARRAY:
        return ENCODER + ".writeArrayStart(); " +
               ENCODER + ".setItemCount(" + name + ".size()); " +
               "for (" + entryType(field.schema()) + " e : " + name + ") { " +
               fieldSingleEncoder("e", field.schema().getElementType()) + " } " +
               ENCODER + ".writeArrayEnd(); ";

      case MAP:
        return ENCODER + ".writeMapStart(); " +
               ENCODER + ".setItemCount(" + name + ".size()); " +
               "for (" + entryType(field.schema()) + " e : " + name + ".entrySet()) { " +
               ENCODER + ".writeString(e.getKey());" +
               fieldSingleEncoder("e.getValue()", field.schema().getValueType()) + " } " +
               ENCODER + ".writeMapEnd(); ";
      default:
        return fieldSingleEncoder(name, field.schema());
    }
  }

  public String fieldSingleEncoder(String name, Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return ENCODER + ".writeBoolean(" + name + ");";
      case BYTES:
        return ENCODER + ".writeBytes(" + name + ");";
      case DOUBLE:
        return ENCODER + ".writeDouble(" + name + ");";
      case ENUM:
        return ENCODER + ".writeEnum(" + name + ".ordinal());";
      case FLOAT:
        return ENCODER + ".writeFloat(" + name + ");";
      case INT:
        return ENCODER + ".writeInt(" + name + ");";
      case LONG:
        return ENCODER + ".writeLong(" + name + ");";
      case STRING:
        return ENCODER + ".writeString(" + name + ");";
      case RECORD:
        return name + ".encode(" + ENCODER + ");";
      default:
        throw new RuntimeException("Unsupported type: " + schema);
    }
  }

  static final public String DECODER = "_decoder";

  public String fieldDecoder(Schema.Field field){
    final String name = mangle(field.name());
    String oc = "_i" + field.pos();
    String ic = "_j" + field.pos();
    switch (field.schema().getType()) {
      case ARRAY:
        return "long " + oc + " = " + DECODER + ".readArrayStart(); " +
               name + " = new org.apache.avro.generic.GenericData.Array<" +
               entryType(field.schema()) + ">((int) " + oc + ", " +
               "SCHEMA$.getFields().get(" + field.pos() + ").schema()); " +
               "for(; " + oc + " != 0; " + oc + " = " + DECODER + ".arrayNext()) { " +
               " for (long " + ic + "= 0; " + ic + " < " + oc + "; " + ic + "++) { " +
               name + ".add(" + fieldSingleDecoder(field.schema().getElementType()) + "); } }";
      case MAP:
        return "long " + oc + " = " + DECODER + ".readMapStart(); " +
               name + " = new java.util.LinkedHashMap<>((int) " + oc + "); " +
               "for(; " + oc + " != 0; " + oc + " = " + DECODER + ".mapNext()) { " +
               " for (long " + ic + "= 0; " + ic + " < " + oc + "; " + ic + "++) { " +
               name + ".put(" + DECODER + ".readString(null), " +
               fieldSingleDecoder(field.schema().getValueType()) + "); } }";
      default:
        return name + " = " + fieldSingleDecoder(field.schema()) + ";";
    }
  }

  public String fieldSingleDecoder(Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN:
        return DECODER + ".readBoolean()";
      case BYTES:
        return DECODER + ".readBytes(null)";
      case DOUBLE:
        return DECODER + ".readDouble()";
      case ENUM:
        return entryType(schema) + ".fromInteger(" + DECODER + ".readEnum())";
      case FLOAT:
        return DECODER + ".readFloat()";
      case INT:
        return DECODER + ".readInt()";
      case LONG:
        return DECODER + ".readLong()";
      case STRING:
        return DECODER + ".readString(null)";
      case RECORD:
        return "new " + entryType(schema) + "().decode(" + DECODER + ")";
      default:
        throw new RuntimeException("Unsupported type: " + schema);
    }
  }
}
