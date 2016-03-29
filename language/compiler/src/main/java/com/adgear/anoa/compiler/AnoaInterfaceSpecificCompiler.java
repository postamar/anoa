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

  public boolean isWithProtobuf() {
    return withProtobuf;
  }

  public boolean isWithThrift() {
    return withThrift;
  }

  @Override
  public String anoaInterfaceName(Schema schema) {
    return mangle(schema.getName());
  }

  @Override
  public String anoaInterfaceFullName(Schema schema) {
    return mangle(schema.getFullName());
  }

  public String avroClassName(Schema schema) {
    return anoaInterfaceFullName(schema) + "Avro";
  }

  public String protobufClassName(Schema schema) {
    String ns = schema.getNamespace();
    ns += "." + ns.substring(ns.lastIndexOf('.') + 1) + "_protobuf";
    String protobufClassName = AnoaParserBase.capitalizeQualified(ns);
    return mangle(AnoaParserBase.capitalizeQualified(protobufClassName + "." + schema.getName()));
  }

  public String thriftClassName(Schema schema) {
    return mangle(schema.getFullName()) + "Thrift";
  }

  static String ARG = "o";

  public String protobufValue(Schema schema, Schema.Field field) {
    String s = ARG + "." + generateGetMethod(schema, field).replace("$", "");
    switch (field.schema().getType()) {
      case ARRAY:
        switch (field.schema().getElementType().getType()) {
          case ENUM:
          case RECORD:
            return "java.util.Collections.unmodifiableList(" + s + "List().stream().map(" +
                   mangle(field.schema().getElementType().getFullName()) + "::fromProtobuf)" +
                   ".collect(java.util.stream.Collectors.toList()))";
          case BYTES:
            return "java.util.Collections.unmodifiableList(" + s + "List().stream()" +
                   ".map(v -> v.asReadOnlyByteBuffer())" +
                   ".collect(java.util.stream.Collectors.toList()))";
          default:
            return s + "List()";
        }
      case MAP:
        switch (field.schema().getValueType().getType()) {
          case ENUM:
          case RECORD:
            return "java.util.Collections.unmodifiableMap(" + s + "().entrySet().stream()" +
                   ".collect(java.util.stream.Collectors.toMap(e -> e.getKey(), e -> " +
                   mangle(field.schema().getValueType().getFullName()) +
                   ".fromProtobuf(e.getValue())))";
          case BYTES:
            return "java.util.Collections.unmodifiableMap(" + s + "().entrySet().stream()" +
                   ".collect(java.util.stream.Collectors.toMap(" +
                   "e -> e.getKey(), " +
                   "e -> e.getValue().asReadOnlyByteBuffer())))";
          default:
            return s + "()";
        }
      case ENUM:
      case RECORD:
        return mangle(field.schema().getFullName()) + ".fromProtobuf(" + s + "())";
      case BYTES:
        return s + "().asReadOnlyByteBuffer()";
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
            s += ".map(java.nio.ByteBuffer::asReadOnlyBuffer)";
            break;
          case FLOAT:
            s += ".map(d -> (float) d)";
        }
        return s + ".collect(java.util.stream.Collectors.toList())";
      case MAP:
        s += ".entrySet().stream().collect(java.util.stream.Collectors.toMap(e -> e.getKey(),";
        Schema v = field.schema().getValueType();
        switch (v.getType()) {
          case ENUM:
          case RECORD:
            s += "e -> " + mangle(v.getFullName()) + ".fromThrift(e.getValue())";
            break;
          case BYTES:
            s += "e -> e.getValue().asReadOnlyBuffer()";
            break;
          case FLOAT:
            s += "e -> (float) e.getValue()";
            break;
          default:
            s += "e -> e.getValue()";
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
