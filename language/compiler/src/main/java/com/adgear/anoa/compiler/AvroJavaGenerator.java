package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

public class AvroJavaGenerator extends JavaGeneratorBase {

  public AvroJavaGenerator(Protocol protocol) {
    super(protocol);
    setTemplateDir("/com/adgear/anoa/avro/");
  }

  @Override
  public String anoaInterfaceName(Schema schema) {
    return super.anoaInterfaceName(schema)
        .substring(0, super.anoaInterfaceName(schema).length() - 4);
  }

  @Override
  public String anoaInterfaceFullName(Schema schema) {
    return super.anoaInterfaceFullName(schema)
        .substring(0, super.anoaInterfaceFullName(schema).length() - 4);
  }


  public boolean hasView(Schema.Field field) {
    switch (field.schema().getType()) {
      case ARRAY:
      case BYTES:
      case MAP:
      case STRING:
        return true;
      default:
        return false;
    }
  }

  public String avroType(Schema schema) {
    return getType(schema, false, false, false);
  }

  private String entryType(Schema schema) {
    return getType(schema, false, false, true);
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
        return entryType(schema) + ".values()[" + DECODER + ".readEnum()]";
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
