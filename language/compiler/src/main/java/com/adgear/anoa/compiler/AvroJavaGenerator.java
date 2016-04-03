package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

public class AvroJavaGenerator extends JavaGeneratorBase {

  public AvroJavaGenerator(Protocol protocol) {
    super(protocol);
    setTemplateDir("/com/adgear/anoa/avro/");
  }

  public String escapedSchema(Schema schema) {
    return javaEscape(CompilationUnit.modifySchema(schema, "", false).toString());
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

  protected String avroInnerType(Schema s) {
    switch (s.getType()) {
      case STRING:  return "org.apache.avro.util.Utf8";
      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return "java.lang.Integer";
      case LONG:    return "java.lang.Long";
      case FLOAT:   return "java.lang.Float";
      case DOUBLE:  return "java.lang.Double";
      case BOOLEAN: return "java.lang.Boolean";
      default:      return anoaInterfaceFullName(s) + "Avro";
    }
  }

  public String avroType(Schema s) {
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

  public boolean hasExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case ARRAY:
      case MAP:
        return true;
      default:
        return false;
    }
  }

  public String exportValue(Schema.Field field) {
    String bytesSupplier = "byte[] b = new byte[bb.remaining()]; "
                           + "bb.asReadOnlyBuffer().get(b); "
                           + "return (java.util.function.Supplier<byte[]>)(b::clone);";
    switch (field.schema().getType()) {
      case BYTES:
        return "java.util.Optional.of(" + mangle(field.name()) + ")"
               + ".map(bb -> {" + bytesSupplier + "}).get()";
      case ARRAY:
        switch (field.schema().getElementType().getType()) {
          case BYTES:
            return "java.util.Collections.unmodifiableList(" + mangle(field.name()) + ".stream()"
                   + ".map(bb -> {" + bytesSupplier + "})"
                   + ".collect(java.util.stream.Collectors.toCollection("
                   + "() -> new java.util.ArrayList<>(" + mangle(field.name()) + ".size()))))";
          case STRING:
          case ENUM:
          case RECORD:
            return "(" + exportType(field.schema()) + ")(java.util.List<?>) "
                   + "java.util.Collections.unmodifiableList(" + mangle(field.name()) + ")";
          default:
            return "java.util.Collections.unmodifiableList(" + mangle(field.name()) + ")";
        }
      case MAP:
        if (field.schema().getValueType().getType() == Schema.Type.BYTES) {
          return "java.util.Collections.unmodifiableMap(" + mangle(field.name()) + ".entrySet()"
                 + ".stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> (java.lang.CharSequence) e.getKey(), "
                 + "e -> { java.nio.ByteBuffer bb = e.getValue(); " + bytesSupplier + " }, "
                 + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap<java.lang.CharSequence,"
                 + "java.util.function.Supplier<byte[]>>(" + mangle(field.name()) + ".size()))))";
        }
        return "(" + exportType(field.schema())+ ")(java.util.Map<?,?>) "
               + "java.util.Collections.unmodifiableMap(" + mangle(field.name()) + ")";
    }
    throw new IllegalStateException();
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


  public String avroValue(Schema schema, Schema.Field field) {
    String get = IMPORTED + "." + generateGetMethod(schema, field) + "()";
    switch (field.schema().getType()) {
      case BYTES:
        return "java.nio.ByteBuffer.wrap(" + get + ".get())";
      case STRING:
        return "new org.apache.avro.util.Utf8(" + get + ".toString())";
      case ENUM:
      case RECORD:
        return avroType(field.schema()) + ".from(" + get + ").get()";
      case ARRAY:
        String map = "";
        switch (field.schema().getElementType().getType()) {
          case BYTES:
            map = ".map(java.util.function.Supplier::get).map(java.nio.ByteBuffer::wrap)";
            break;
          case STRING:
            map = ".map(Object::toString).map(org.apache.avro.util.Utf8::new)";
            break;
          case ENUM:
          case RECORD:
            map = ".map(" + avroType(field.schema().getElementType()) + "::from)"
                  + ".map(java.util.function.Supplier::get)";
            break;
        }
        return get + ".stream()" + map + ".collect(java.util.stream.Collectors.toCollection(() -> "
               + "new " + avroType(field.schema()) + "(" + get + ".size(), SCHEMA$.getFields().get("
               + field.pos() + ").schema())))";
      case MAP:
        String fn = "e -> e.getValue()";
        switch (field.schema().getValueType().getType()) {
          case BYTES:
            fn = "e -> java.nio.ByteBuffer.wrap(e.getValue().get())";
            break;
          case STRING:
            fn = "e -> new org.apache.avro.util.Utf8(e.getValue().toString())";
            break;
          case ENUM:
          case RECORD:
            fn = "e -> " + avroType(field.schema().getValueType()) + ".from(e.getValue()).get()";
            break;
        }
        return get + ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
               + "e -> new org.apache.avro.util.Utf8(e.getKey().toString()), "
               + fn + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new " + avroType(field.schema()) + "(" + get + ".size())))";
      default:
        return get;
    }
  }
}
