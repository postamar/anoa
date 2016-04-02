package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

public class InterfaceJavaGenerator extends JavaGeneratorBase {

  final public boolean withAvro;
  final public boolean withProtobuf;
  final public boolean withThrift;

  public InterfaceJavaGenerator(Protocol protocol,
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
    return anoaInterfaceFullName(schema) + "Avro";
  }

  public boolean isWithProtobuf() {
    return withProtobuf;
  }

  public String protobufClassName(Schema schema) {
    String ns = schema.getNamespace();
    ns += "." + ns.substring(ns.lastIndexOf('.') + 1) + "_protobuf";
    String protobufClassName = AnoaParserBase.capitalizeQualified(ns);
    return mangle(AnoaParserBase.capitalizeQualified(protobufClassName + "." + schema.getName()));
  }

  public String protobufProtocolClassName() {
    return protocolFullName + "Protobuf";
  }

  public boolean isWithThrift() {
    return withThrift;
  }

  public String thriftClassName(Schema schema) {
    return anoaInterfaceFullName(schema) + "Thrift";
  }

  public String cast(Schema.Field field) {
    switch (field.schema().getType()) {
      case MAP:
        return "(" + exportType(field.schema()) + ")(java.util.Map<?,?>) ";
      case ARRAY:
        return "(" + exportType(field.schema()) + ")(java.util.List<?>) ";
    }
    return "";
  }

  public boolean hasProtobufExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case ENUM:
      case RECORD:
        return true;
      case ARRAY:
        switch (field.schema().getElementType().getType()) {
          case BYTES:
          case ENUM:
          case RECORD:
            return true;
        }
        return false;
      case MAP:
        switch (field.schema().getValueType().getType()) {
          case BYTES:
          case ENUM:
          case RECORD:
            return true;
        }
        return false;
    }
    return false;
  }

  public String protobufExportValue(Schema schema, Schema.Field field) {
    String base = "get()." + generateGetMethod(schema, field).replace("$", "");
    String value = base + ((field.schema().getType() == Schema.Type.ARRAY) ? "List()" : "()");
    if (!hasProtobufExportField(field)) {
      switch (field.schema().getType()) {
        case MAP:
          return "(" + exportType(field.schema()) + ")(java.util.Map<?,?>) " + value;
        case ARRAY:
          if (field.schema().getElementType().getType() == Schema.Type.STRING) {
            return "(java.util.List<java.lang.CharSequence>)(java.util.List<?>) " + value;
          }
      }
      return value;
    }
    switch (field.schema().getType()) {
      case ARRAY:
        return "java.util.Collections.unmodifiableList(" + value + ".stream().map("
               + ((field.schema().getElementType().getType() == Schema.Type.BYTES)
                  ? "bs -> new java.util.function.Supplier<byte[]>() { "
                    + "public byte[] get() { return bs.toByteArray();} }"
                  : mangle(field.schema().getElementType().getFullName()) + ".Protobuf::from")
               + ").collect(java.util.stream.Collectors.toCollection("
               + "() -> new java.util.ArrayList<>(" + base + "Count()))))";
      case MAP:
        return "java.util.Collections.unmodifiableMap(" + value + ".entrySet().stream().collect("
               + "java.util.stream.Collectors.toMap(e -> e.getKey(), e -> "
               + ((field.schema().getValueType().getType() == Schema.Type.BYTES)
                  ? "{ com.google.protobuf.ByteString bs = e.getValue(); "
                    + "return new java.util.function.Supplier<byte[]>() { "
                    + "public byte[] get() { return bs.toByteArray();} }; }, "
                  : mangle(field.schema().getValueType().getFullName())
                    + ".Protobuf.from(e.getValue()), ")
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.HashMap<java.lang.CharSequence,"
               + exportType(field.schema().getValueType()) + ">(" + value + ".size()))))";

      case BYTES:
        return "() -> " + value + ".toByteArray()";
      default:
        return mangle(field.schema().getFullName()) + ".Protobuf.from(" + value + ")";
    }
  }

  public String protobufImportField(Schema schema, Schema.Field field) {
    String setter = generateSetMethod(schema, field).replace("$", "");
    String value = "instance." + generateGetMethod(schema, field) + "()";
    switch (field.schema().getType()) {
      case BYTES:
        value = "com.google.protobuf.ByteString.copyFrom(" + value + ".get())";
        break;
      case STRING:
        value += ".toString()";
        break;
      case ENUM:
      case RECORD:
        value = mangle(field.schema().getFullName()) + ".Protobuf.from(" + value + ").get()";
        break;
      case ARRAY:
        setter = "addAll" + setter.substring(3);
        String map = "";
        switch (field.schema().getElementType().getType()) {
          case BYTES:
            map = ".map(java.util.function.Supplier::get)"
                  + ".map(com.google.protobuf.ByteString::copyFrom)";
            break;
          case STRING:
            map = ".map(java.lang.Object::toString)";
            break;
          case ENUM:
          case RECORD:
            map = ".map(" + mangle(field.schema().getElementType().getFullName())
                  + ".Protobuf::from).map(java.util.function.Supplier::get)";
        }
        if (!map.isEmpty()) {
          value += ".stream()" + map + ".collect(java.util.stream.Collectors.toCollection("
                   + "() -> new java.util.ArrayList(" + value + ".size())))";
        }
        break;
      case MAP:
        setter = "putAll" + setter.substring(3);
        String fn = "e -> e.getValue()";
        switch (field.schema().getValueType().getType()) {
          case BYTES:
            fn = "e -> com.google.protobuf.ByteString.copyFrom(e.getValue().get())";
            break;
          case STRING:
            fn = "e -> e.getValue().toString()";
            break;
          case ENUM:
          case RECORD:
            fn = "e -> " + mangle(field.schema().getValueType().getFullName())
                 + ".Protobuf.from(e.getValue()).get()";
        }
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey().toString(), " + fn + ", (u,v) -> { "
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap<>(" + value + ".size())))";
        break;
    }
    return "." + setter + "(" + value + ")";
  }

  public boolean hasThriftExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case STRING:
      case ENUM:
      case RECORD:
      case ARRAY:
      case MAP:
        return true;
    }
    return false;
  }

  public String thriftExportValue(Schema.Field field) {
    String value = "wrapped.get" + Character.toUpperCase(field.name().charAt(0))
                   + field.name().substring(1) + "()";
    if (!hasThriftExportField(field)) {
      return ((field.schema().getType() == Schema.Type.FLOAT) ? "(float) " : "") + value;
    }
    String bytesSupplier = "byte[] b = new byte[bb.remaining()]; "
                           + "bb.asReadOnlyBuffer().get(b); "
                           + "return (java.util.function.Supplier<byte[]>)(b::clone);";
    value = "java.util.Optional.ofNullable(" + value + ")";
    switch (field.schema().getType()) {
      case BYTES:
        return value + ".orElseGet(() -> new byte[0])::clone";
      case STRING:
        return value + ".orElse(\"\")";
      case ENUM:
        return mangle(field.schema().getFullName()) + ".Thrift.from(" + value
               + ".map(org.apache.thrift.TEnum::getValue).orElse(0))";
      case RECORD:
        return mangle(field.schema().getFullName()) + ".Thrift.from(" + value + ".orElseGet("
               + thriftClassName(field.schema()) + "::new))";
      case ARRAY:
        String map = "";
        switch (field.schema().getElementType().getType()) {
          case BYTES:
            map = ".map(bb -> {" + bytesSupplier + "})";
            break;
          case FLOAT:
            map = ".map(d -> d.floatValue())";
            break;
          case ENUM:
          case RECORD:
            map = ".map(" + mangle(field.schema().getElementType().getFullName())
                   + ".Thrift::from)";
            break;
        }
        return value + ".map(l -> l.stream()" + map + ".collect("
               + "java.util.stream.Collectors.toCollection("
               + "() -> new java.util.ArrayList<>(l.size()))))"
               + ".orElseGet(() -> new java.util.ArrayList<>())";
      case MAP:
        String fn = "e -> e.getValue()";
        switch (field.schema().getValueType().getType()) {
          case BYTES:
            fn = "e -> { java.nio.ByteBuffer bb = e.getValue(); " + bytesSupplier + " }";
            break;
          case FLOAT:
            fn = "e -> e.getValue().floatValue()";
            break;
          case ENUM:
          case RECORD:
            fn = "e -> " + mangle(field.schema().getValueType().getFullName()) +
                 ".Thrift.from(e.getValue())";
            break;
        }
        return value + ".map(m -> m.entrySet().stream().collect("
               + "java.util.stream.Collectors.toMap("
               + "e -> e.getKey().toString(), "
               + fn + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.HashMap<java.lang.CharSequence,"
               + exportValueType(field.schema().getValueType()) + ">(m.size()))))"
               + ".orElseGet(() -> new java.util.HashMap<>())";
      default:
        return value;
    }
  }

  public String thriftImportValue(Schema schema, Schema.Field field) {
    String value = IMPORTED + "." + generateGetMethod(schema, field) + "()";
    switch (field.schema().getType()) {
      case FLOAT:
        return "(double) " + value;
      case BYTES:
        return "java.nio.ByteBuffer.wrap(" + value + ".get())";
      case STRING:
        return value + ".toString()";
      case ENUM:
      case RECORD:
        return mangle(field.schema().getFullName()) + ".Thrift.from(" + value + ").get()";
      case ARRAY:
        String map = "";
        switch (field.schema().getElementType().getType()) {
          case FLOAT:
            map = ".map(java.lang.Float::doubleValue)";
            break;
          case BYTES:
            map = ".map(java.util.function.Supplier::get).map(java.nio.ByteBuffer::wrap)";
            break;
          case STRING:
            map = ".map(java.lang.Object::toString)";
            break;
          case ENUM:
          case RECORD:
            map = ".map(" + mangle(field.schema().getElementType().getFullName()) + ".Thrift::from)"
                  + ".map(java.util.function.Supplier::get)";
            break;
        }
        return value + ".stream()" + map + ".collect(java.util.stream.Collectors.toCollection(() ->"
               + " new java.util.ArrayList<>(" + value + ".size())))";
      case MAP:
        String fn = "e -> e.getValue()";
        switch (field.schema().getValueType().getType()) {
          case FLOAT:
            fn = "e -> e.getValue().doubleValue()";
            break;
          case BYTES:
            fn = "e -> java.nio.ByteBuffer.wrap(e.getValue().get())";
            break;
          case STRING:
            fn = "e -> e.getValue().toString()";
            break;
          case ENUM:
          case RECORD:
            fn = "e -> " + mangle(field.schema().getValueType().getFullName())
                 + ".Thrift.from(e.getValue()).get()";
            break;
        }
        return value + ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
               + "e -> e.getKey().toString(), "
               + fn + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.HashMap<>(" + value + ".size())))";
    }
    return value;
  }
}
