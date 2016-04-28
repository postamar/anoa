package com.adgear.anoa.compiler.javagen;

import com.adgear.anoa.compiler.CompilationUnit;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Custom java source code generator for Avro enums and SpecificRecords.
 */
public class AvroJavaGenerator extends AbstractJavaGenerator {

  public AvroJavaGenerator(Protocol protocol) {
    super(protocol);
    setTemplateDir("/com/adgear/anoa/avro/");
  }

  public String escapedSchema(Schema schema) throws IOException {
    return javaSplit(CompilationUnit.modifySchema(schema, "", false).toString());
  }

  public boolean isRecursiveFreeze(Schema.Field field) {
    switch (field.schema().getType()) {
      case RECORD:
        return true;
      case ARRAY:
        return (field.schema().getElementType().getType() == Schema.Type.RECORD);
      case MAP:
        return (field.schema().getValueType().getType() == Schema.Type.RECORD);
    }
    return false;
  }

  public String recursiveFreeze(Schema.Field field) {
    switch (field.schema().getType()) {
      case ARRAY:
        return mangle(field.name()) + ".forEach(e -> e.freeze())";
      case MAP:
        return mangle(field.name()) + ".forEach((k, v) -> v.freeze())";
      default:
        return mangle(field.name()) + ".freeze()";
    }
  }

  static final public String ENCODER = "_encoder";

  public String fieldEncoder(Schema.Field field){
    final String name = mangle(field.name());
    switch (field.schema().getType()) {
      case ARRAY:
        return ENCODER + ".writeArrayStart(); "
               + ENCODER + ".setItemCount(" + name + ".size()); "
               + "for (" + avroEntryType(field.schema()) + " e : " + name + ") { "
               + fieldSingleEncoder("e", field.schema().getElementType()) + " } "
               + ENCODER + ".writeArrayEnd(); ";

      case MAP:
        return ENCODER + ".writeMapStart(); "
               + ENCODER + ".setItemCount(" + name + ".size()); "
               + "for (" + avroEntryType(field.schema()) + " e : " + name + ".entrySet()) { "
               + ENCODER + ".writeString(e.getKey());"
               + fieldSingleEncoder("e.getValue()", field.schema().getValueType()) + " } "
               + ENCODER + ".writeMapEnd(); ";
      default:
        return fieldSingleEncoder(name, field.schema());
    }
  }

  protected String fieldSingleEncoder(String name, Schema schema) {
    switch (schema.getType()) {
      case BOOLEAN: return ENCODER + ".writeBoolean(" + name + ");";
      case BYTES:   return ENCODER + ".writeBytes(" + name + ");";
      case DOUBLE:  return ENCODER + ".writeDouble(" + name + ");";
      case ENUM:    return ENCODER + ".writeEnum(" + name + ".ordinal());";
      case FLOAT:   return ENCODER + ".writeFloat(" + name + ");";
      case INT:     return ENCODER + ".writeInt(" + name + ");";
      case LONG:    return ENCODER + ".writeLong(" + name + ");";
      case STRING:  return ENCODER + ".writeString(" + name + ");";
      case RECORD:  return name + ".encode(" + ENCODER + ");";
    }
    throw new IllegalStateException();
  }

  static final public String DECODER = "_decoder";

  public String fieldDecoder(Schema.Field field){
    final String name = mangle(field.name());
    String oc = "_i" + field.pos();
    String ic = "_j" + field.pos();
    switch (field.schema().getType()) {
      case ARRAY:
        return "long " + oc + " = " + DECODER + ".readArrayStart(); "
               + name + " = new org.apache.avro.generic.GenericData.Array<"
               + avroEntryType(field.schema()) + ">((int) " + oc + ", SCHEMA$.getFields().get("
               + field.pos() + ").schema()); "
               + "for(; " + oc + " != 0; " + oc + " = " + DECODER + ".arrayNext()) { "
               + "for (long " + ic + "= 0; " + ic + " < " + oc + "; " + ic + "++) { "
               + name + ".add(" + fieldSingleDecoder(field.schema().getElementType(), null)
               + "); } }";
      case MAP:
        return "long " + oc + " = " + DECODER + ".readMapStart(); "
               + name + " = new java.util.HashMap<>((int) " + oc + "); "
               + "for(; " + oc + " != 0; " + oc + " = " + DECODER + ".mapNext()) { "
               + "for (long " + ic + "= 0; " + ic + " < " + oc + "; " + ic + "++) { "
               + name + ".put(" + DECODER + ".readString(null), "
               + fieldSingleDecoder(field.schema().getValueType(), null) + "); } }";
      default:
        return name + " = " + fieldSingleDecoder(field.schema(), field) + ";";
    }
  }

  protected String fieldSingleDecoder(Schema schema, Schema.Field field) {
    String fname = field == null ? "null" : mangle(field.name());
    switch (schema.getType()) {
      case BOOLEAN: return DECODER + ".readBoolean()";
      case DOUBLE:  return DECODER + ".readDouble()";
      case ENUM:    return avroType(schema) + ".values()[" + DECODER + ".readEnum()]";
      case FLOAT:   return DECODER + ".readFloat()";
      case INT:     return DECODER + ".readInt()";
      case LONG:    return DECODER + ".readLong()";
      case STRING:  return DECODER + ".readString(" + fname + ")";
      case BYTES:   return DECODER + ".readBytes(" + fname + ")";
      case RECORD:
        return (field == null ? ("new " + avroType(schema) + "()") : (
            "java.util.Optional.ofNullable(" + fname + ").orElseGet(" + avroType(schema) + "::new)"
        )) + ".decode(" + DECODER + ")";
    }
    throw new IllegalStateException();
  }


  static protected String avroInnerType(Schema s) {
    switch (s.getType()) {
      case STRING:  return "org.apache.avro.util.Utf8";
      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return "java.lang.Integer";
      case LONG:    return "java.lang.Long";
      case FLOAT:   return "java.lang.Float";
      case DOUBLE:  return "java.lang.Double";
      case BOOLEAN: return "java.lang.Boolean";
      default:      return  mangle(s.getFullName());
    }
  }

  static public String avroType(Schema s) {
    switch (s.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return s.getType().toString().toLowerCase();
      case ARRAY:
        return "org.apache.avro.generic.GenericData.Array<"
               + avroInnerType(s.getElementType()) + ">";
      case MAP:
        return "java.util.HashMap<org.apache.avro.util.Utf8,"
               + avroInnerType(s.getValueType()) + ">";
      default:
        return avroInnerType(s);
    }
  }

  protected String avroEntryType(Schema s) {
    if (s.getType() == Schema.Type.MAP) {
      return "java.util.Map.Entry<org.apache.avro.util.Utf8,"
             + avroInnerType(s.getValueType()) + ">";
    }
    return avroInnerType(s.getElementType());
  }
}
