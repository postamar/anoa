package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

public class AnoaInterfaceSpecificCompiler extends AnoaAvroSpecificCompiler {

  final public boolean withAvro;
  final public boolean withProtobuf;
  final public boolean withThrift;

  public AnoaInterfaceSpecificCompiler(Protocol protocol,
                                       boolean withAvro,
                                       boolean withProtobuf,
                                       boolean withThrift) {
    super(protocol);
    setTemplateDir("/com/adgear/anoa/interface/");
    this.withAvro = withAvro;
    this.withProtobuf = withProtobuf;
    this.withThrift = withThrift;
  }

  public boolean isWithAvro() {
    return withAvro;
  }

  public String avroClassName(Schema schema) {
    return mangle(schema.getFullName()) + "Avro";
  }

  public boolean isWithProtobuf() {
    return withProtobuf;
  }

  public String protobufClassName(Schema schema) {
    String lcus = schema.getNamespace();
    lcus += "." + lcus.substring(lcus.lastIndexOf('.') + 1) + "_protobuf";
    String protobufClassName = AnoaParserBase.capitalizeQualified(lcus);
    return mangle(AnoaParserBase.capitalizeQualified(protobufClassName + "." + schema.getName()));
  }

  public boolean isWithThrift() {
    return withThrift;
  }

  public String thriftClassName(Schema schema) {
    return mangle(schema.getFullName()) + "Thrift";
  }

  public String anoaType(Schema schema) {
    return anoaType(schema, false);
  }

  public String anoaType(Schema schema, boolean boxed) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
        return mangle(schema.getFullName());
      case ARRAY:
        return "java.util.List<" + anoaType(schema.getElementType(), true) + ">";
      case MAP:
        return "java.util.Map<java.lang.CharSequence," + anoaType(schema.getValueType(), true) + ">";
      case STRING:  return "java.lang.CharSequence";
      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return boxed ? "java.lang.Integer" : "int";
      case LONG:    return boxed ? "java.lang.Long" : "long";
      case FLOAT:   return boxed ? "java.lang.Float" : "float";
      case DOUBLE:  return boxed ? "java.lang.Double" : "double";
      case BOOLEAN: return boxed ? "java.lang.Boolean" : "boolean";
      default: throw new RuntimeException("Unsupported type: " + schema);
    }
  }

  public String anoaGetter(Schema schema, Schema.Field field) {
    return generateGetMethod(schema, field);
  }

  static public String ARG = "o";

  public String avroValue(Schema schema, Schema.Field field) {
    String s = ARG + "." + mangle(field.name());
    switch (field.schema().getType()) {
      case ARRAY:
        s += ".stream()";
        Schema e = field.schema().getElementType();
        switch (e.getType()) {
          case ENUM:
          case RECORD:
            s += ".map(" + mangle(e.getFullName()) + "::fromAvro)";
            break;
          case BYTES:
            s += ".map(java.nio.ByteBuffer::asReadOnlyBuffer)";
            break;
        }
        return s + ".collect(java.util.stream.Collectors.toList())";
      case MAP:
        s += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
             + "java.util.Map.Entry::getKey,";
        Schema v = field.schema().getValueType();
        switch (v.getType()) {
          case ENUM:
          case RECORD:
            s += "e -> " + mangle(v.getFullName()) + ".fromAvro(e.getValue())";
            break;
          case BYTES:
            s += "e -> e.getValue().asReadOnlyBuffer()";
            break;
          default:
            s += "java.util.Map.Entry::getValue";
        }
        return s + "))";
      case BYTES:
        return s + ".asReadOnlyBuffer()";
      case ENUM:
      case RECORD:
        return mangle(field.schema().getFullName()) + ".fromAvro(" + s + ")";
      default:
        return s;
    }
  }


  public String protobufValue(Schema schema, Schema.Field field) {
    String s = ARG + "." + generateGetMethod(schema, field);
    switch (field.schema().getType()) {
      case ARRAY:
        s += "List().stream()";
        Schema e = field.schema().getElementType();
        switch (e.getType()) {
          case ENUM:
          case RECORD:
            s += ".map(" + mangle(e.getFullName()) + "::fromProtobuf)";
            break;
          case BYTES:
            s += ".map(com.google.protobuf.ByteString::asReadOnlyByteBuffer)";
            break;
        }
        return s + ".collect(java.util.stream.Collectors.toList())";
      case MAP:
        s += "().entrySet().stream().collect(java.util.stream.Collectors.toMap("
             + "java.util.Map.Entry::getKey,";
        Schema v = field.schema().getValueType();
        switch (v.getType()) {
          case ENUM:
          case RECORD:
            s += "e -> " + mangle(v.getFullName()) + ".fromProtobuf(e.getValue())";
            break;
          case BYTES:
            s += "e -> e.getValue().asReadOnlyByteBuffer()";
            break;
          default:
            s += "java.util.Map.Entry::getValue";
        }
        return s + "))";
      case BYTES:
        return s + "().asReadOnlyByteBuffer()";
      case ENUM:
      case RECORD:
        return mangle(field.schema().getFullName()) + ".fromProtobuf(" + s + "())";
      default:
        return s + "()";
    }
  }

  public String thriftValue(Schema schema, Schema.Field field) {
    String s = ARG + ".get"
               + Character.toUpperCase(field.name().charAt(0)) + field.name().substring(1) + "()";
    switch (field.schema().getType()) {
      case ARRAY:
        s += ".stream()";
        Schema e = field.schema().getElementType();
        switch (e.getType()) {
          case ENUM:
          case RECORD:
            s += ".map(" + mangle(e.getFullName()) + "::fromThrift)";
            break;
          case BYTES:
            s += ".map(java.nio.ByteBuffer::wrap).map(java.nio.ByteBuffer::asReadOnlyBuffer)";
            break;
          case FLOAT:
            s += ".map(d -> (float) d)";
        }
        return s + ".collect(java.util.stream.Collectors.toList())";
      case MAP:
        s += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
             + "java.util.Map.Entry::getKey,";
        Schema v = field.schema().getValueType();
        switch (v.getType()) {
          case ENUM:
          case RECORD:
            s += "e -> " + mangle(v.getFullName()) + ".fromThrift(e.getValue())";
            break;
          case BYTES:
            s += "e -> java.nio.ByteBuffer.wrap(e.getValue()).asReadOnlyBuffer()";
            break;
          case FLOAT:
            s += "e -> (float) e.getValue()";
            break;
          default:
            s += "java.util.Map.Entry::getValue";
        }
        return s + "))";
      case BYTES:
        return "java.nio.ByteBuffer.wrap(" + s + ").asReadOnlyBuffer()";
      case FLOAT:
        return "(float) " + s;
      case ENUM:
      case RECORD:
        return mangle(field.schema().getFullName()) + ".fromThrift(" + s + ")";
      default:
        return s;
    }
  }
}
